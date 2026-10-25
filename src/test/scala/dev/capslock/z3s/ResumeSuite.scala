package dev.capslock.z3s

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import dev.capslock.z3s.age.X25519Identity
import java.nio.file.{Files, Path}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*
import scodec.bits.ByteVector

/** 中断したバックアップの再開を、フェイクのzfsとS3で確かめる */
class ResumeSuite extends IOSpec:
  val dataset = "tank/data"
  val snapshot = Snapshot(s"$dataset@daily-1", 101, 10)
  val objectKey = s"backups/$dataset@daily-1"
  val MiB = 1024 * 1024
  val raw = SendMode.Raw
  val plain = SendMode.Plain(Compression.None, Encryption.None)

  final case class Env(zfs: FakeZfs, store: InMemoryStore, ops: Ops, dir: Path, states: StateStore)

  /** 12 MiBのストリームは5 MiBずつ3パートに分かれる */
  def env: IO[Env] =
    for
      ds <- Ref.of[IO, Map[String, List[Snapshot]]](Map(dataset -> List(snapshot)))
      dir <- IO.blocking(Files.createTempDirectory("z3s-state"))
    yield
      val zfs = FakeZfs(ds, 12 * MiB)
      val store = InMemoryStore()
      val common = Common("bucket", "backups/", dataset, None, dryRun = false, concurrency = 1)
      Env(zfs, store, Ops(common, zfs, store, Duration.Zero), dir, StateStore(dir, _ => IO.unit))

  def backup(e: Env, mode: SendMode, noResume: Boolean = false): IO[Unit] =
    e.ops.backup(BackupOptions(full = true, Some("daily-1"), mode, None, noResume, Some(e.dir)))

  /** 3パート目以降のUploadPartを失敗させて中断し、失敗のエラーを返す */
  def interrupt(e: Env, mode: SendMode): IO[Throwable] =
    e.store.failPartsFrom.set(Some(3)) *> backup(e, mode).attempt.map(_.left.value) <*
      e.store.failPartsFrom.set(None) <* e.store.uploadPartCalls.set(Nil)

  def stateFiles(dir: Path): IO[List[Path]] =
    IO.blocking(Files.list(dir).iterator.asScala.toList)

  for (name, mode) <- List("raw" -> raw, "plain without encryption" -> plain) do
    test(s"an interrupted $name backup resumes without uploading finished parts again") {
      for
        e <- env
        error <- interrupt(e, mode)
        saved <- e.states.load("bucket", objectKey)
        _ <- backup(e, mode)
        resent <- e.store.uploadPartCalls.get
        aborted <- e.store.aborted.get
        index <- e.store.getWithEtag("backups/tank/data/.z3s-index.cbor").map(o => Cbor.decodeIndex(o.value._1))
        left <- stateFiles(e.dir)
        _ <- e.ops.restore(RestoreOptions("daily-1", Some("pool/r"), force = false, None))
        received <- e.zfs.received.get
      yield
        error.getMessage should include("injected failure for part 3")
        saved.value.parts.map(_.number) should === (List(1, 2))
        resent should === (List(3))
        aborted should === (Nil)
        // 再開したバックアップは、中断前のヘッダ(createdAtを含む)をそのまま使う
        Cbor.encodeHeader(index.value.entries.head.header).toSeq should === (saved.value.header.toSeq)
        left should === (Nil)
        received.map(_._2.toList) should === (List(e.zfs.streamFor(snapshot, None, mode == raw).toList))
    }

  test("an interrupted age-encrypted backup is aborted and leaves no state") {
    for
      id <- X25519Identity.generate
      e <- env
      mode = SendMode.Plain(Compression.None, Encryption.Age(NonEmptyList.one(id.recipient.encode)))
      _ <- interrupt(e, mode)
      aborted <- e.store.aborted.get
      pending <- e.store.uploads.get
      files <- stateFiles(e.dir)
    yield
      aborted should have size 1
      pending should === (Map.empty[String, e.store.PendingUpload])
      files should === (Nil)
  }

  test("a part that differs from the record aborts the upload, and the next run starts over") {
    for
      e <- env
      _ <- interrupt(e, raw)
      saved <- e.states.load("bucket", objectKey)
      _ <- e.states.save(saved.value.copy(parts = saved.value.parts.map(_.copy(md5 = "0" * 32))))
      mismatch <- backup(e, raw).attempt
      aborted <- e.store.aborted.get
      afterMismatch <- stateFiles(e.dir)
      _ <- backup(e, raw)
      sent <- e.store.uploadPartCalls.get
    yield
      inside(mismatch.left.value) { case u: Z3SError.Unresumable =>
        u.getMessage should include("differs from the interrupted upload")
      }
      aborted should === (List(saved.value.uploadId))
      afterMismatch should === (Nil)
      sent should === (List(1, 2, 3))
  }

  test("--no-resume discards the interrupted upload and starts over") {
    for
      e <- env
      _ <- interrupt(e, raw)
      saved <- e.states.load("bucket", objectKey)
      _ <- backup(e, raw, noResume = true)
      aborted <- e.store.aborted.get
      sent <- e.store.uploadPartCalls.get
    yield
      aborted should === (List(saved.value.uploadId))
      sent should === (List(1, 2, 3))
  }

  test("a different mode discards the interrupted upload") {
    for
      e <- env
      _ <- interrupt(e, raw)
      saved <- e.states.load("bucket", objectKey)
      _ <- backup(e, plain)
      aborted <- e.store.aborted.get
      index <- e.store.getWithEtag("backups/tank/data/.z3s-index.cbor").map(o => Cbor.decodeIndex(o.value._1))
    yield
      aborted should === (List(saved.value.uploadId))
      index.value.entries.map(_.header.mode) should === (List(plain))
  }

  test("an upload removed from S3 is started over") {
    for
      e <- env
      _ <- interrupt(e, raw)
      // lifecycleルールでuploadが削除された状態を作る
      _ <- e.store.uploads.set(Map.empty)
      _ <- backup(e, raw)
      sent <- e.store.uploadPartCalls.get
      files <- stateFiles(e.dir)
    yield
      sent should === (List(1, 2, 3))
      files should === (Nil)
  }

  test("state files round trip, their removal is logged, and unreadable ones are ignored") {
    for
      dir <- IO.blocking(Files.createTempDirectory("z3s-state"))
      logs <- Ref.of[IO, List[String]](Nil)
      states = StateStore(dir, m => logs.update(_ :+ m))
      state = UploadState("b", "k", "id", 5 * MiB, ByteVector(1, 2, 3), List(PartRecord(1, "ab" * 16, 5L * MiB)))
      _ <- states.save(state)
      loaded <- states.load("b", "k")
      other <- states.load("b", "other")
      _ <- IO.blocking(Files.writeString(states.path("b", "k"), "{broken"))
      broken <- states.load("b", "k")
      _ <- states.delete("b", "k")
      messages <- logs.get
    yield
      loaded should === (Some(state))
      other should === (None)
      broken should === (None)
      messages should have size 2
      messages(0) should startWith("warning: ignoring unreadable resume state")
      messages(1) should === (s"removed resume state ${states.path("b", "k")}")
  }
