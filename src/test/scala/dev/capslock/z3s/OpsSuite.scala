package dev.capslock.z3s

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import munit.CatsEffectSuite
import dev.capslock.z3s.age.X25519Identity

/** zfsとS3をフェイクに置き換え、backupからrestoreまでを実行する */
class OpsSuite extends CatsEffectSuite:
  val dataset = "tank/data"
  val snapshots = List(
    Snapshot(s"$dataset@daily-1", 101, 10),
    Snapshot(s"$dataset@hourly-1", 201, 15),
    Snapshot(s"$dataset@daily-2", 102, 20),
    Snapshot(s"$dataset@daily-3", 103, 30)
  )

  def common(dryRun: Boolean = false) =
    Common("bucket", "backups/", dataset, Some("daily"), dryRun = dryRun, concurrency = 3)

  final case class Env(zfs: FakeZfs, store: InMemoryStore, ops: Ops)

  def env(payloadSize: Int = 200000): IO[Env] =
    Ref.of[IO, Map[String, List[Snapshot]]](Map(dataset -> snapshots)).map { ds =>
      val zfs = FakeZfs(ds, payloadSize)
      val store = InMemoryStore()
      Env(zfs, store, Ops(common(), zfs, store))
    }

  def backupOpts(mode: SendMode, full: Boolean, snapshot: Option[String] = None) =
    BackupOptions(full, snapshot, mode, None)

  def restoreOpts(snapshot: String, target: String, ids: Option[NonEmptyList[X25519Identity]] = None) =
    RestoreOptions(snapshot, Some(target), force = false, ids)

  def loadIndex(store: InMemoryStore): IO[Index] =
    store.getWithEtag("backups/tank/data/.z3s-index.cbor").map(o => Cbor.decodeIndex(o.get._1).toOption.get)

  /** 復元先が受け取ったストリームが、元のsend出力と一致することを確かめる */
  def assertReceived(e: Env, target: String, expected: List[(String, Option[String], Boolean)]): IO[Unit] =
    e.zfs.received.get.map { got =>
      val want = expected.map { (name, parent, raw) =>
        e.zfs.streamFor(snapshots.find(_.shortName == name).get, parent.map(p => s"$dataset@$p"), raw).toList
      }
      assertEquals(got.map(_._1), expected.map(_ => target))
      assertEquals(got.map(_._2.toList), want)
    }

  val modes: List[(String, IO[(SendMode, Option[NonEmptyList[X25519Identity]])])] =
    List(
      "raw" -> IO.pure((SendMode.Raw, None)),
      "plain none" -> IO.pure((SendMode.Plain(Compression.None, Encryption.None), None)),
      "plain gzip" -> IO.pure((SendMode.Plain(Compression.Gzip(6), Encryption.None), None)),
      "plain zstd" -> IO.pure((SendMode.Plain(Compression.Zstd(3), Encryption.None), None)),
      "plain xz" -> IO.pure((SendMode.Plain(Compression.Xz(1), Encryption.None), None)),
      "plain bzip2" -> IO.pure((SendMode.Plain(Compression.Bzip2(9), Encryption.None), None)),
      "plain zstd + age" -> X25519Identity.generate.map { id =>
        (SendMode.Plain(Compression.Zstd(3), Encryption.Age(NonEmptyList.one(id.recipient.encode))), Some(NonEmptyList.one(id)))
      }
    )

  for (name, setup) <- modes do
    test(s"full and incremental backup, then restore: $name") {
      for
        (mode, ids) <- setup
        e <- env()
        raw = mode == SendMode.Raw
        _ <- e.ops.backup(backupOpts(mode, full = true, Some("daily-1")))
        _ <- e.ops.backup(backupOpts(mode, full = false))
        index <- loadIndex(e.store)
        _ = assertEquals(index.entries.map(_.header.snapshot), List("daily-1", "daily-2", "daily-3").map(s => s"$dataset@$s"))
        _ = assertEquals(index.entries.map(_.header.parent.map(_.guid)), List(None, Some(101L), Some(102L)))
        _ <- e.ops.restore(restoreOpts("daily-3", "pool/restored", ids))
        _ <- assertReceived(
          e,
          "pool/restored",
          List(("daily-1", None, raw), ("daily-2", Some("daily-1"), raw), ("daily-3", Some("daily-2"), raw))
        )
      yield ()
    }

  test("mixing compression across the chain is allowed") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Plain(Compression.Gzip(1), Encryption.None), full = true, Some("daily-1")))
      _ <- e.ops.backup(backupOpts(SendMode.Plain(Compression.Xz(0), Encryption.None), full = false, Some("daily-2")))
      _ <- e.ops.restore(restoreOpts("daily-2", "pool/r"))
      _ <- assertReceived(e, "pool/r", List(("daily-1", None, false), ("daily-2", Some("daily-1"), false)))
    yield ()
  }

  test("mixing raw and plain is rejected with exit code 3") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true, Some("daily-1")))
      r <- e.ops.backup(backupOpts(SendMode.Plain(Compression.None, Encryption.None), full = false)).attempt
    yield assertEquals(r.left.toOption.collect { case z: Z3SError => z.exitCode }, Some(3))
  }

  test("incremental without a base asks for --full") {
    env().flatMap(_.ops.backup(backupOpts(SendMode.Raw, full = false)).attempt).map { r =>
      assert(r.left.exists(_.getMessage.contains("--full")), r.toString)
    }
  }

  test("restore resumes after the latest snapshot that the target already has") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true, Some("daily-1")))
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false, Some("daily-2")))
      _ <- e.ops.restore(restoreOpts("daily-2", "pool/r"))
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false))
      _ <- e.ops.restore(restoreOpts("daily-3", "pool/r"))
      got <- e.zfs.received.get
    yield assertEquals(got.size, 3)
  }

  test("backup is a no-op when the target is already on S3") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true))
      before <- e.store.objects.get
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false))
      after <- e.store.objects.get
    yield assertEquals(after.keySet, before.keySet)
  }

  test("an existing object is not overwritten") {
    for
      e <- env()
      _ <- e.store.put("backups/tank/data@daily-3", Array[Byte](1, 2, 3))
      r <- e.ops.backup(backupOpts(SendMode.Raw, full = true)).attempt
      obj <- e.store.objects.get.map(_("backups/tank/data@daily-3"))
    yield
      assert(r.isLeft)
      assertEquals(obj.bytes.toList, List[Byte](1, 2, 3))
  }

  test("reindex rebuilds the index from headers and skips foreign objects") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true, Some("daily-1")))
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false))
      original <- loadIndex(e.store)
      _ <- e.store.put("backups/tank/data@not-ours", "hello".getBytes)
      _ <- e.store.objects.update(_ - "backups/tank/data/.z3s-index.cbor")
      rebuilt <- e.ops.reindex
    yield
      assertEquals(rebuilt.entries.map(_.header), original.entries.map(_.header))
      assertEquals(rebuilt.entries.map(_.size), original.entries.map(_.size))
  }

  test("a missing index is rebuilt before backup") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true, Some("daily-1")))
      _ <- e.store.objects.update(_ - "backups/tank/data/.z3s-index.cbor")
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false))
      index <- loadIndex(e.store)
    yield assertEquals(index.entries.size, 3)
  }

  test("a concurrent index update is merged") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true, Some("daily-1")))
      // 次のインデックス書き込みの直前に、別プロセスによるentryの追加を再現する
      foreign = IndexEntry(
        "backups/tank/data@other",
        Header("tank/data@other", 999, 99, None, SendMode.Raw, 0, "", ""),
        0,
        "",
        ""
      )
      _ <- e.store.beforePut.set(Some(IndexStore(e.store, common().indexKey).update(_.upsert(foreign)).void))
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false, Some("daily-2")))
      index <- loadIndex(e.store)
    yield assertEquals(index.entries.map(_.key).toSet, Set("daily-1", "daily-2", "other").map(s => s"backups/tank/data@$s"))
  }

  test("tampered objects fail to restore") {
    def tamper(e: Env, key: String, f: Array[Byte] => Array[Byte]) =
      e.store.objects.update(m => m.updated(key, m(key).copy(bytes = f(m(key).bytes.clone()))))
    for
      id <- X25519Identity.generate
      mode = SendMode.Plain(Compression.Zstd(3), Encryption.Age(NonEmptyList.one(id.recipient.encode)))
      e <- env()
      _ <- e.ops.backup(backupOpts(mode, full = true, Some("daily-1")))
      key = "backups/tank/data@daily-1"
      original <- e.store.objects.get.map(_(key).bytes)
      results <- List[Array[Byte] => Array[Byte]](
        b => { b(b.length - 10) = (b(b.length - 10) ^ 1).toByte; b }, // payloadを書き換える
        b => b.dropRight(100), // 切り詰める
        b => { b(40) = (b(40) ^ 1).toByte; b } // 平文ヘッダを書き換える
      ).traverse { f =>
        tamper(e, key, _ => f(original.clone())) *>
          e.ops.restore(restoreOpts("daily-1", "pool/r", Some(NonEmptyList.one(id)))).attempt
      }
      got <- e.zfs.received.get
    yield
      results.foreach(r => assert(r.isLeft, r.toString))
      assertEquals(got, Nil)
  }

  test("restore without --identity for an age object is a usage error") {
    for
      id <- X25519Identity.generate
      e <- env()
      _ <- e.ops.backup(
        backupOpts(SendMode.Plain(Compression.None, Encryption.Age(NonEmptyList.one(id.recipient.encode))), full = true)
      )
      r <- e.ops.restore(restoreOpts("daily-3", "pool/r")).attempt
    yield assertEquals(r.left.toOption.collect { case z: Z3SError => z.exitCode }, Some(2))
  }

  test("dry run writes nothing") {
    for
      e <- env()
      dry = Ops(common(dryRun = true), e.zfs, e.store)
      _ <- dry.backup(backupOpts(SendMode.Raw, full = true))
      objects <- e.store.objects.get
    yield assertEquals(objects, Map.empty)
  }

  test("part size follows the estimate") {
    for
      e <- env(payloadSize = 12 * 1024 * 1024)
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true))
      parts <- e.store.partSizes.get
    yield assertEquals(parts.dropRight(1).distinct, List(5 * 1024 * 1024))
  }

  test("status --json renders empty and populated results") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true, Some("daily-1")))
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false))
      index <- loadIndex(e.store)
    yield
      assertEquals(StatusRow.json(Nil), "[]")
      val rows = index.entries.map(en => StatusRow(en, Health.Ok, local = true))
      val json = StatusRow.json(rows)
      assert(json.startsWith("[{\"snapshot\":\"tank/data@daily-1\""), json)
      assertEquals("\"health\":\"ok\"".r.findAllMatchIn(json).size, 3)
  }

  test("reindex rescans when the index changes during the scan") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true, Some("daily-1")))
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = false))
      key = "backups/tank/data@daily-3"
      obj <- e.store.objects.get.map(_(key))
      entry <- loadIndex(e.store).map(_.entries.find(_.key == key).get)
      _ <- e.store.objects.update(_ - key)
      // スキャンの後、インデックスを書く直前に別プロセスがdaily-3をアップロードしたことにする
      _ <- e.store.beforePut.set(Some(
        e.store.objects.update(_.updated(key, obj)) *>
          IndexStore(e.store, common().indexKey).update(_.upsert(entry)).void
      ))
      rebuilt <- e.ops.reindex
      stored <- loadIndex(e.store)
    yield
      assertEquals(rebuilt.entries.map(_.key).last, key)
      assertEquals(stored.entries.map(_.key), rebuilt.entries.map(_.key))
  }

  test("reindex replaces a malformed index") {
    for
      e <- env()
      _ <- e.ops.backup(backupOpts(SendMode.Raw, full = true))
      _ <- e.store.put(common().indexKey, Array[Byte](0x7f, 0x00))
      rebuilt <- e.ops.reindex
      stored <- loadIndex(e.store)
    yield assertEquals(stored.entries.map(_.key), rebuilt.entries.map(_.key))
  }

  test("restore needs --identity only for encrypted objects that are still to be received") {
    for
      id <- X25519Identity.generate
      e <- env()
      encrypted = SendMode.Plain(Compression.None, Encryption.Age(NonEmptyList.one(id.recipient.encode)))
      plain = SendMode.Plain(Compression.None, Encryption.None)
      _ <- e.ops.backup(backupOpts(encrypted, full = true, Some("daily-1")))
      _ <- e.ops.restore(restoreOpts("daily-1", "pool/r", Some(NonEmptyList.one(id))))
      _ <- e.ops.backup(backupOpts(plain, full = false))
      _ <- e.ops.restore(restoreOpts("daily-3", "pool/r"))
      _ <- e.ops.restore(restoreOpts("daily-3", "pool/r"))
      got <- e.zfs.received.get
    yield assertEquals(got.size, 3)
  }
