package dev.capslock.z3s

import cats.effect.IO
import cats.syntax.all.*
import dev.capslock.z3s.codec.resume.UploadStateCodec
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
          .map(UploadStateCodec.decode)
          .flatMap {
            case Right(s) if s.bucket == bucket && s.key == key => IO.pure(Some(s))
            case Right(_) => log(s"warning: ignoring resume state $p for a different object").as(None)
            case Left(e)  => log(s"warning: ignoring unreadable resume state $p: $e").as(None)
          }
    }

  def save(state: UploadState): IO[Unit] =
    val p = path(state.bucket, state.key)
    IO.blocking {
      Files.createDirectories(dir)
      val tmp = Files.createTempFile(dir, ".tmp-", ".json")
      Files.write(tmp, UploadStateCodec.encode(state))
      Files.move(tmp, p, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    }.void

  def delete(bucket: String, key: String): IO[Unit] =
    val p = path(bucket, key)
    IO.blocking(Files.deleteIfExists(p)).flatMap(deleted => log(s"removed resume state $p").whenA(deleted))
