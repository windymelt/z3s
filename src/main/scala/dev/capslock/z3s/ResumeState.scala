package dev.capslock.z3s

import cats.effect.IO
import cats.syntax.all.*
import io.bullet.borer.{Decoder, Encoder, Json}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.MessageDigest
import scodec.bits.ByteVector

/** アップロード済みのパート。md5は再開時に作り直したパートと照合する */
final case class PartRecord(number: Int, md5: Md5, size: Long)

/** 中断したmultipart uploadを再開するための状態。暗号化しないmodeでだけ保存するので秘密を含まない */
final case class UploadState(
    bucket: String,
    key: String,
    uploadId: String,
    partSize: Int,
    header: ByteVector,
    parts: List[PartRecord]
):
  def record(part: PartRecord): UploadState =
    copy(parts = (parts.filterNot(_.number == part.number) :+ part).sortBy(_.number))

object UploadState:
  val SchemaVersion = 1

  given Encoder[UploadState] = Encoder { (w, s) =>
    // borerのJSON出力は長さ付きのarrayとmapに対応しないため、不定長で書く
    w.writeMapStart()
    w.writeString("schemaVersion").writeInt(SchemaVersion)
    w.writeString("bucket").writeString(s.bucket)
    w.writeString("key").writeString(s.key)
    w.writeString("uploadId").writeString(s.uploadId)
    w.writeString("partSize").writeInt(s.partSize)
    w.writeString("header").writeString(s.header.toBase64)
    w.writeString("parts").writeArrayStart()
    s.parts.foreach { p =>
      w.writeMapStart()
      w.writeString("number").writeInt(p.number)
      w.writeString("md5").writeString(p.md5.hex)
      w.writeString("size").writeLong(p.size)
      w.writeBreak()
    }
    w.writeBreak()
    w.writeBreak()
  }

  given Decoder[PartRecord] = Decoder { r =>
    var number: Option[Int] = None
    var md5: Option[Md5] = None
    var size: Option[Long] = None
    Cbor.readMap(r) {
      case "number" => r => number = Some(r.readInt())
      case "md5" =>
        r =>
          val hex = r.readString()
          md5 = Some(Md5.fromHex(hex).getOrElse(Cbor.fail(s"invalid MD5: $hex")))
      case "size"   => r => size = Some(r.readLong())
    }
    PartRecord(Cbor.required("number", number), Cbor.required("md5", md5), Cbor.required("size", size))
  }

  given Decoder[UploadState] = Decoder { r =>
    var version: Option[Int] = None
    var bucket: Option[String] = None
    var key: Option[String] = None
    var uploadId: Option[String] = None
    var partSize: Option[Int] = None
    var header: Option[ByteVector] = None
    var parts: List[PartRecord] = Nil
    Cbor.readMap(r) {
      case "schemaVersion" => r => version = Some(r.readInt())
      case "bucket"        => r => bucket = Some(r.readString())
      case "key"           => r => key = Some(r.readString())
      case "uploadId"      => r => uploadId = Some(r.readString())
      case "partSize"      => r => partSize = Some(r.readInt())
      case "header" =>
        r => header = Some(ByteVector.fromBase64(r.readString()).getOrElse(Cbor.fail("invalid header")))
      case "parts" => r => parts = Cbor.readArray(r)(_.read[PartRecord]())
    }
    if !version.contains(SchemaVersion) then Cbor.fail(s"unsupported schemaVersion: $version")
    UploadState(
      Cbor.required("bucket", bucket),
      Cbor.required("key", key),
      Cbor.required("uploadId", uploadId),
      Cbor.required("partSize", partSize),
      Cbor.required("header", header),
      parts
    )
  }

/** 状態をオブジェクトごとに1ファイルとして保存する。削除したときにログを出す */
final class StateStore(val dir: Path, log: String => IO[Unit]):
  def path(bucket: String, key: String): Path =
    val digest = MessageDigest.getInstance("SHA-256").digest(s"$bucket\n$key".getBytes(UTF_8))
    dir.resolve(ByteVector(digest).toHex.take(32) + ".json")

  /** 読めない状態ファイルは警告して無視し、最初からアップロードする */
  def load(bucket: String, key: String): IO[Option[UploadState]] =
    val p = path(bucket, key)
    IO.blocking(Files.exists(p)).flatMap {
      case false => IO.pure(None)
      case true =>
        IO.blocking(Files.readAllBytes(p))
          .map(bytes => Json.decode(bytes).to[UploadState].valueEither)
          .flatMap {
            case Right(s) if s.bucket == bucket && s.key == key => IO.pure(Some(s))
            case Right(_) => log(s"warning: ignoring resume state $p for a different object").as(None)
            case Left(e)  => log(s"warning: ignoring unreadable resume state $p: ${e.getMessage}").as(None)
          }
    }

  def save(state: UploadState): IO[Unit] =
    val p = path(state.bucket, state.key)
    IO.blocking {
      Files.createDirectories(dir)
      val tmp = Files.createTempFile(dir, ".tmp-", ".json")
      Files.write(tmp, Json.encode(state).toByteArray)
      Files.move(tmp, p, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    }.void

  def delete(bucket: String, key: String): IO[Unit] =
    val p = path(bucket, key)
    IO.blocking(Files.deleteIfExists(p)).flatMap(deleted => log(s"removed resume state $p").whenA(deleted))
