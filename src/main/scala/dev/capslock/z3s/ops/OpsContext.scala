package dev.capslock.z3s.ops

import cats.effect.{Clock, IO}
import dev.capslock.z3s.*
import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** 各処理が共有する、1つのdatasetについての依存と出力 */
private[ops] final class OpsContext(
    val common: Common,
    val zfs: Zfs,
    val store: ObjectStore,
    val uploadRetryBase: FiniteDuration
):
  val indexStore: IndexStore = IndexStore(store, common.indexKey)
  val progress: Progress = Progress(ProgressMode.resolve(common.progress))
  val uploader: MultipartUploader = MultipartUploader(store, common.concurrency, uploadRetryBase, progress.log)
  def out(s: String): IO[Unit] = IO.println(s)
  def err(s: String): IO[Unit] = IO.consoleForIO.errorln(s)
  def now: IO[Instant] = Clock[IO].realTimeInstant

  def sha256(bytes: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(bytes)

  /** 同じ設定で、別のdatasetを扱う */
  def forDataset(dataset: String): OpsContext = OpsContext(common.copy(dataset = dataset), zfs, store, uploadRetryBase)
