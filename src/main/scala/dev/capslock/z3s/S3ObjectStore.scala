package dev.capslock.z3s

import cats.effect.{IO, Resource}
import fs2.{Chunk, Stream}
import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, CompletionException}
import org.reactivestreams.FlowAdapters
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.core.checksums.{RequestChecksumCalculation, ResponseChecksumValidation}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}
import software.amazon.awssdk.services.s3.model.*

final class S3ObjectStore(client: S3AsyncClient, bucket: String) extends ObjectStore:

  // SDKのメソッドはFutureを返すまで呼び出し元のスレッドを使い、初回は100ms以上かかることがあるので、IO.blockingで呼ぶ
  private def call[A](f: => CompletableFuture[A]): IO[A] =
    IO.fromCompletableFuture(IO.blocking(f)).adaptError { case e: CompletionException if e.getCause != null => e.getCause }

  private def status(e: Throwable): Option[Int] = e match
    case s: S3Exception => Some(s.statusCode())
    case _              => None

  private def isPreconditionFailure(e: Throwable): Boolean =
    // 同時書き込みが競合した場合の409 ConditionalRequestConflictも、412と同じく再試行する
    status(e).exists(c => c == 412 || c == 409)

  def list(prefix: String): Stream[IO, ObjectInfo] =
    def page(token: Option[String]): IO[ListObjectsV2Response] =
      val b = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix)
      token.foreach(b.continuationToken)
      call(client.listObjectsV2(b.build()))

    Stream
      .unfoldLoopEval(Option.empty[String])(page(_).map { r =>
        val next = Option(r.nextContinuationToken()).filter(_ => r.isTruncated)
        (r.contents().asScala.toList, next.map(Some(_)))
      })
      .flatMap(Stream.emits)
      .map(o => ObjectInfo(o.key(), o.size(), o.eTag(), o.lastModified()))

  def head(key: String): IO[Option[ObjectInfo]] =
    call(client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()))
      .map(r => Option(ObjectInfo(key, r.contentLength(), r.eTag(), r.lastModified())))
      .recover { case e if status(e).contains(404) => None }

  def getPrefix(key: String, length: Int): IO[Array[Byte]] =
    val req = GetObjectRequest.builder().bucket(bucket).key(key).range(s"bytes=0-${length - 1}").build()
    call(client.getObject(req, AsyncResponseTransformer.toBytes[GetObjectResponse]()))
      .map(_.asByteArray())
      // 空のオブジェクトにbytes=0-のrangeを指定すると416になる
      .recover { case e if status(e).contains(416) => Array.emptyByteArray }

  def get(key: String): Stream[IO, Byte] =
    val req = GetObjectRequest.builder().bucket(bucket).key(key).build()
    Stream
      .eval(call(client.getObject(req, AsyncResponseTransformer.toPublisher[GetObjectResponse]())))
      .flatMap { publisher =>
        fs2.interop.flow.fromPublisher[IO, ByteBuffer](chunkSize = 16) { subscriber =>
          IO(publisher.subscribe(FlowAdapters.toSubscriber(subscriber)))
        }
      }
      .flatMap(bb => Stream.chunk(Chunk.byteBuffer(bb)))

  def getWithEtag(key: String): IO[Option[(Array[Byte], String)]] =
    val req = GetObjectRequest.builder().bucket(bucket).key(key).build()
    call(client.getObject(req, AsyncResponseTransformer.toBytes[GetObjectResponse]()))
      .map(r => Option((r.asByteArray(), r.response().eTag())))
      .recover { case e if status(e).contains(404) => None }

  private def put(b: PutObjectRequest.Builder, bytes: Array[Byte]): IO[PutResult] =
    call(client.putObject(b.bucket(bucket).build(), AsyncRequestBody.fromBytes(bytes)))
      .map(r => PutResult.Written(r.eTag()))
      .recover { case e if isPreconditionFailure(e) => PutResult.PreconditionFailed }

  def putIfAbsent(key: String, bytes: Array[Byte]): IO[PutResult] =
    put(PutObjectRequest.builder().key(key).ifNoneMatch("*"), bytes)

  def putIfMatch(key: String, bytes: Array[Byte], etag: String): IO[PutResult] =
    put(PutObjectRequest.builder().key(key).ifMatch(etag), bytes)

  def createMultipartUpload(key: String, metadata: Map[String, String], storageClass: Option[String]): IO[String] =
    val b = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).metadata(metadata.asJava)
    storageClass.foreach(b.storageClass)
    call(client.createMultipartUpload(b.build())).map(_.uploadId())

  def uploadPart(key: String, uploadId: String, number: Int, bytes: Array[Byte]): IO[String] =
    val req = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(number).build()
    call(client.uploadPart(req, AsyncRequestBody.fromBytesUnsafe(bytes))).map(_.eTag())

  def listParts(key: String, uploadId: String): IO[Option[List[UploadedPart]]] =
    // 1回の応答は最大1000パートなので、マーカーを進めて読む
    def page(marker: Option[Int]): IO[ListPartsResponse] =
      val b = ListPartsRequest.builder().bucket(bucket).key(key).uploadId(uploadId)
      marker.foreach(m => b.partNumberMarker(m))
      call(client.listParts(b.build()))

    Stream
      .unfoldLoopEval(Option.empty[Int])(page(_).map { r =>
        val next = Option(r.nextPartNumberMarker()).map(_.toInt).filter(_ => r.isTruncated)
        (r.parts().asScala.toList, next.map(Some(_)))
      })
      .flatMap(Stream.emits)
      .map(p => UploadedPart(p.partNumber(), p.eTag(), p.size()))
      .compile
      .toList
      .map(Option(_))
      .recover { case e if status(e).contains(404) => None }

  def completeMultipartUpload(key: String, uploadId: String, parts: List[UploadedPart]): IO[String] =
    val completed = parts.sortBy(_.number).map(p => CompletedPart.builder().partNumber(p.number).eTag(p.etag).build())
    val req = CompleteMultipartUploadRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .uploadId(uploadId)
      .multipartUpload(CompletedMultipartUpload.builder().parts(completed.asJava).build())
      .ifNoneMatch("*")
      .build()
    call(client.completeMultipartUpload(req)).map(_.eTag()).adaptError {
      case e if isPreconditionFailure(e) => Z3SError.Unresumable(s"object already exists: s3://$bucket/$key", e)
    }

  def abortMultipartUpload(key: String, uploadId: String): IO[Unit] =
    call(
      client.abortMultipartUpload(
        AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).build()
      )
    ).void

object S3ObjectStore:
  def client(region: Option[String], endpoint: Option[URI]): Resource[IO, S3AsyncClient] =
    // クライアントの作成は設定ファイルの読み込みなどで数百msかかることがあるので、IO.blockingで行う
    Resource.fromAutoCloseable(IO.blocking {
      // さくらのオブジェクトストレージでは、Expect: 100-continueを付けたUploadPartが
      // Status Code: 100で失敗し、付けない場合は成功した
      val b = S3AsyncClient
        .builder()
        .serviceConfiguration(S3Configuration.builder().expectContinueEnabled(false).build())
      region.foreach(r => b.region(Region.of(r)))
      endpoint.foreach { uri =>
        // S3互換ストレージにはvirtual-hosted styleやSDK既定のチェックサムに対応しないものがある
        b.endpointOverride(uri)
          .forcePathStyle(true)
          .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
          .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
      }
      b.build()
    })
