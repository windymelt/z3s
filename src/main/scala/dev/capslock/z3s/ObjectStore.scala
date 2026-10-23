package dev.capslock.z3s

import cats.effect.IO
import fs2.Stream
import java.time.Instant

final case class ObjectInfo(key: String, size: Long, etag: String, lastModified: Instant)

enum PutResult:
  case Written(etag: String)
  case PreconditionFailed

final case class UploadResult(etag: String, size: Long)

final case class UploadedPart(number: Int, etag: String, size: Long)

trait ObjectStore:
  def list(prefix: String): Stream[IO, ObjectInfo]
  def head(key: String): IO[Option[ObjectInfo]]
  /** `[0, length)`を取得する。オブジェクトが短い場合は全体を返す */
  def getPrefix(key: String, length: Int): IO[Array[Byte]]
  def get(key: String): Stream[IO, Byte]
  /** 内容とETagを返す。存在しなければNone */
  def getWithEtag(key: String): IO[Option[(Array[Byte], String)]]
  def putIfAbsent(key: String, bytes: Array[Byte]): IO[PutResult]
  def putIfMatch(key: String, bytes: Array[Byte], etag: String): IO[PutResult]
  /** uploadIdを返す */
  def createMultipartUpload(key: String, metadata: Map[String, String], storageClass: Option[String]): IO[String]
  /** ETagを返す */
  def uploadPart(key: String, uploadId: String, number: Int, bytes: Array[Byte]): IO[String]
  /** アップロード済みのパートを返す。uploadが存在しなければNone */
  def listParts(key: String, uploadId: String): IO[Option[List[UploadedPart]]]
  /** 同名のオブジェクトが既にある場合はZ3SError.Unresumableで失敗し、上書きしない */
  def completeMultipartUpload(key: String, uploadId: String, parts: List[UploadedPart]): IO[String]
  def abortMultipartUpload(key: String, uploadId: String): IO[Unit]
