package dev.capslock.z3s

import cats.effect.{IO, Resource}
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, CompletionException}
import org.reactivestreams.FlowAdapters
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.core.checksums.{RequestChecksumCalculation, ResponseChecksumValidation}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}
import software.amazon.awssdk.services.s3.model.*

final class S3ObjectStore(client: S3AsyncClient, bucket: String) extends ObjectStore:
  private val MaxParts = 10000
  private val UploadPartRetries = 5

  private def call[A](f: => CompletableFuture[A]): IO[A] =
    IO.fromCompletableFuture(IO(f)).adaptError { case e: CompletionException if e.getCause != null => e.getCause }

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

  def uploadMultipart(
      key: String,
      metadata: Map[String, String],
      storageClass: Option[String],
      partSize: Int,
      concurrency: Int,
      data: Stream[IO, Byte]
  ): IO[UploadResult] =
    val create =
      val b = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).metadata(metadata.asJava)
      storageClass.foreach(b.storageClass)
      call(client.createMultipartUpload(b.build())).map(_.uploadId())

    def abort(uploadId: String): IO[Unit] =
      call(
        client.abortMultipartUpload(
          AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).build()
        )
      ).void.handleErrorWith(e =>
        IO.consoleForIO.errorln(s"warning: failed to abort multipart upload $uploadId for $key: ${e.getMessage}")
      )

    def uploadPart(uploadId: String, number: Int, bytes: Array[Byte]): IO[CompletedPart] =
      val req = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(number).build()
      val once = call(client.uploadPart(req, AsyncRequestBody.fromBytesUnsafe(bytes)))
        .map(r => CompletedPart.builder().partNumber(number).eTag(r.eTag()).build())
      def attempt(n: Int): IO[CompletedPart] =
        once.handleErrorWith { e =>
          if n >= UploadPartRetries then IO.raiseError(e)
          else
            val delay = (1L << n).seconds
            IO.consoleForIO.errorln(s"warning: UploadPart $number failed (${e.getMessage}); retrying in $delay") *>
              IO.sleep(delay) *> attempt(n + 1)
        }
      attempt(0)

    def complete(uploadId: String, parts: List[CompletedPart]): IO[String] =
      val req = CompleteMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(parts.asJava).build())
        .ifNoneMatch("*")
        .build()
      call(client.completeMultipartUpload(req)).map(_.eTag()).adaptError {
        case e if isPreconditionFailure(e) => Z3SError.Runtime(s"object already exists: s3://$bucket/$key", e)
      }

    Resource
      .makeCase(create) {
        case (_, ExitCase.Succeeded) => IO.unit
        case (uploadId, _)           => abort(uploadId)
      }
      .use { uploadId =>
        data
          .chunkN(partSize, allowFewer = true)
          .zipWithIndex
          .evalTap { case (_, i) =>
            IO.raiseError(Z3SError.Runtime(s"part count exceeds $MaxParts; the size estimate was too small"))
              .whenA(i >= MaxParts)
          }
          .parEvalMap(concurrency) { case (chunk, i) =>
            uploadPart(uploadId, i.toInt + 1, chunk.toArray).map(p => (p, chunk.size.toLong))
          }
          .compile
          .toList
          .flatMap { parts =>
            complete(uploadId, parts.map(_._1)).map(etag => UploadResult(etag, parts.map(_._2).sum))
          }
      }

object S3ObjectStore:
  def client(region: Option[String], endpoint: Option[URI]): Resource[IO, S3AsyncClient] =
    Resource.fromAutoCloseable(IO {
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
