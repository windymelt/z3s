package dev.capslock.z3s

import cats.effect.{IO, Ref, Resource}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.std.Mutex
import cats.syntax.all.*
import fs2.Stream
import java.security.MessageDigest
import scala.concurrent.duration.*
import scodec.bits.ByteVector

/** 進捗表示に出すパートの数 */
final class PartCounters private (done: Ref[IO, Int], uploading: Ref[IO, Int]):
  def started: IO[Unit] = uploading.update(_ + 1)
  def stopped: IO[Unit] = uploading.update(_ - 1)
  def finished: IO[Unit] = done.update(_ + 1)
  def describe: IO[Option[String]] = (done.get, uploading.get).mapN((d, u) => Some(s"parts $d done, $u uploading"))

object PartCounters:
  def make: IO[PartCounters] = (Ref.of[IO, Int](0), Ref.of[IO, Int](0)).mapN(PartCounters(_, _))

/** 中断したuploadの状態と、S3に残っているパート */
final case class Resume(state: UploadState, uploaded: List[UploadedPart])

/** ストリームをパートに分けてmultipart uploadで送る。状態の保存先があれば、中断時にuploadを残して再開できるようにする */
object MultipartUploader:
  private val DigestSliceSize = 1024 * 1024

  /** パートは100MBを超えることがあるため、分割して計算してほかのファイバーに実行を譲る */
  private[z3s] def md5Hex(bytes: Array[Byte]): IO[String] =
    IO(MessageDigest.getInstance("MD5")).flatMap { md =>
      (0 until bytes.length by DigestSliceSize).toList
        .traverse_(off => IO(md.update(bytes, off, math.min(DigestSliceSize, bytes.length - off))) *> IO.cede) *>
        IO(ByteVector(md.digest()).toHex)
    }

final class MultipartUploader(
    store: ObjectStore,
    concurrency: Int,
    retryBase: FiniteDuration,
    log: String => IO[Unit]
):
  private val MaxParts = 10000
  private val UploadPartRetries = 5

  /** @param states Noneなら、失敗時に常にuploadをabortする
    * @param resume 再開する場合の状態。Noneなら新しくuploadを作る
    */
  def upload(
      bucket: String,
      key: String,
      metadata: Map[String, String],
      storageClass: Option[String],
      partSize: Int,
      header: ByteVector,
      data: Stream[IO, Byte],
      states: Option[StateStore],
      resume: Option[Resume],
      counters: PartCounters
  ): IO[UploadResult] =
    val start: IO[UploadState] = resume match
      case Some(r) => IO.pure(r.state)
      case None =>
        store.createMultipartUpload(key, metadata, storageClass).flatMap { uploadId =>
          val s = UploadState(bucket, key, uploadId, partSize, header, Nil)
          states.traverse_(_.save(s)).as(s)
        }

    def abort(s: UploadState): IO[Unit] =
      store
        .abortMultipartUpload(key, s.uploadId)
        .handleErrorWith(e => log(s"warning: failed to abort multipart upload ${s.uploadId} for $key: ${e.getMessage}"))

    def release(s: UploadState, exit: ExitCase): IO[Unit] = (exit, states) match
      case (ExitCase.Succeeded, _)                           => IO.unit
      case (ExitCase.Errored(_: Z3SError.Unresumable), _) => abort(s) *> states.traverse_(_.delete(bucket, key))
      case (_, None)                                         => abort(s)
      case (_, Some(st)) =>
        log(
          s"kept multipart upload ${s.uploadId} for $key; " +
            s"run the same backup again to resume (state: ${st.path(bucket, key)})"
        )

    def uploadWithRetry(s: UploadState, number: Int, bytes: Array[Byte], md5: String): IO[String] =
      def attempt(n: Int): IO[String] =
        store.uploadPart(key, s.uploadId, number, bytes, md5).handleErrorWith { e =>
          if n >= UploadPartRetries then IO.raiseError(e)
          else
            val delay = retryBase * (1L << n)
            log(s"warning: UploadPart $number failed (${e.getMessage}); retrying in $delay") *>
              IO.sleep(delay) *> attempt(n + 1)
        }
      attempt(0)

    Resource.makeCase(start)(release).use { s0 =>
      val recorded = s0.parts.map(p => p.number -> p).toMap
      val onS3 = resume.toList.flatMap(_.uploaded).map(p => p.number -> p).toMap
      for
        state <- Ref.of[IO, UploadState](s0)
        lock <- Mutex[IO]
        record = (part: PartRecord) =>
          states.traverse_(st => lock.lock.surround(state.updateAndGet(_.record(part)).flatMap(st.save)))

        // 作り直したパートが記録したMD5と一致し、S3上のパートとサイズも一致すれば、送らずにS3上のパートを使う
        reuse = (number: Int, bytes: Array[Byte], md5: String, r: PartRecord, p: UploadedPart) =>
          if r.md5 == md5 && r.size == bytes.length && p.size == bytes.length then counters.finished.as(p)
          else
            IO.raiseError(
              Z3SError.Unresumable(
                s"part $number of $key differs from the interrupted upload; run the backup again to start over"
              )
            )

        send = (number: Int, bytes: Array[Byte], md5: String) =>
          (counters.started *> uploadWithRetry(s0, number, bytes, md5).guarantee(counters.stopped)).flatMap { etag =>
            record(PartRecord(number, md5, bytes.length.toLong)) *>
              counters.finished.as(UploadedPart(number, etag, bytes.length.toLong))
          }

        processPart = (number: Int, bytes: Array[Byte]) =>
          MultipartUploader.md5Hex(bytes).flatMap { md5 =>
            (recorded.get(number), onS3.get(number)) match
              case (Some(r), Some(p)) => reuse(number, bytes, md5, r, p)
              case _                  => send(number, bytes, md5)
          }

        parts <- data
          .chunkN(partSize, allowFewer = true)
          .zipWithIndex
          .evalTap { case (_, i) =>
            IO.raiseError(Z3SError.Unresumable(s"part count exceeds $MaxParts; the size estimate was too small"))
              .whenA(i >= MaxParts)
          }
          .parEvalMap(concurrency) { case (chunk, i) => processPart(i.toInt + 1, chunk.toArray) }
          .compile
          .toList
        etag <- store.completeMultipartUpload(key, s0.uploadId, parts)
        _ <- states.traverse_(_.delete(bucket, key))
      yield UploadResult(etag, parts.map(_.size).sum)
    }
