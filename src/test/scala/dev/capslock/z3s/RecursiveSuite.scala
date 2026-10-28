package dev.capslock.z3s

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import dev.capslock.z3s.age.X25519Identity
import dev.capslock.z3s.codec.index.IndexCodec
import dev.capslock.z3s.ops.{BackupOptions, Ops, RestoreOptions}

/** --recursiveによる子孫のdatasetのバックアップとstatus */
class RecursiveSuite extends IOSpec:
  val zstd = SendMode.Plain(Compression.Zstd(3), Encryption.None)

  def snap(dataset: String, short: String, guid: Long, txg: Long) = Snapshot(s"$dataset@$short", guid, txg)

  val tree: Map[String, List[Snapshot]] = Map(
    "pool/foo" -> List(snap("pool/foo", "s1", 1, 10), snap("pool/foo", "s2", 2, 20)),
    "pool/foo/bar" -> List(snap("pool/foo/bar", "s1", 11, 10), snap("pool/foo/bar", "s2", 12, 20)),
    "pool/foo/baz" -> List(snap("pool/foo/baz", "s1", 21, 10)),
    // 名前の前方一致で紛れ込まないことを確かめるための兄弟
    "pool/foobar" -> List(snap("pool/foobar", "s1", 31, 10))
  )

  final case class Env(zfs: FakeZfs, store: InMemoryStore, ops: Ops)

  def env: IO[Env] =
    Ref.of[IO, Map[String, List[Snapshot]]](tree).flatMap { ds =>
      val zfs = FakeZfs(ds, 1000)
      val store = InMemoryStore()
      val common = Common("bucket", "backups/", "pool/foo", None, dryRun = false, concurrency = 2)
      zfs.nativeEncrypted.set(Set("pool/foo/bar")).as(Env(zfs, store, Ops(common, zfs, store)))
    }

  def backup(e: Env, mode: SendMode = zstd, full: Boolean = false, snapshot: Option[String] = None) =
    e.ops.backup(BackupOptions(full, snapshot, mode, None, recursive = true))

  def entries(e: Env, dataset: String): IO[List[IndexEntry]] =
    e.store.getWithEtag(s"backups/$dataset/.z3s-index.cbor").map(_.toList.flatMap(o => IndexCodec.decode(o._1).toOption.get.entries))

  test("encrypted datasets are sent raw, others in the given mode, and datasets without the snapshot are skipped") {
    for
      e <- env
      _ <- backup(e, full = true)
      sends <- e.zfs.sends.get
      foo <- entries(e, "pool/foo")
      bar <- entries(e, "pool/foo/bar")
      baz <- entries(e, "pool/foo/baz")
      foobar <- e.store.objects.get.map(_.keys.filter(_.startsWith("backups/pool/foobar")).toList)
    yield
      sends should === (List("pool/foo@s2" -> false, "pool/foo/bar@s2" -> true))
      foo.map(e => (e.header.snapshot, e.header.mode)) should === (List("pool/foo@s2" -> zstd))
      bar.map(e => (e.header.snapshot, e.header.mode)) should === (List("pool/foo/bar@s2" -> SendMode.Raw))
      baz should === (Nil)
      foobar should === (Nil)
  }

  test("--raw sends every dataset raw") {
    for
      e <- env
      _ <- backup(e, SendMode.Raw, full = true, snapshot = Some("s1"))
      sends <- e.zfs.sends.get
    yield sends should === (List("pool/foo@s1" -> true, "pool/foo/bar@s1" -> true, "pool/foo/baz@s1" -> true))
  }

  test("a new dataset without backups is sent full, while the others are incremental") {
    for
      e <- env
      _ <- backup(e, full = true, snapshot = Some("s1"))
      _ <- e.zfs.datasets.update(_.updated("pool/foo/new", List(snap("pool/foo/new", "s2", 41, 20))))
      _ <- backup(e)
      foo <- entries(e, "pool/foo")
      created <- entries(e, "pool/foo/new")
    yield
      foo.map(_.header.parent.map(_.name)) should === (List(None, Some("pool/foo@s1")))
      created.map(e => (e.header.snapshot, e.header.parent)) should === (List("pool/foo/new@s2" -> None))
  }

  test("a failing dataset does not stop the others and is reported at the end") {
    for
      e <- env
      _ <- e.zfs.failingSends.set(Set("pool/foo/bar"))
      result <- backup(e, full = true, snapshot = Some("s1")).attempt
      foo <- entries(e, "pool/foo")
      baz <- entries(e, "pool/foo/baz")
    yield
      inside(result.left.value) { case z: Z3SError =>
        z.exitCode should === (1)
        z.getMessage should === ("1 of 3 dataset(s) failed: pool/foo/bar")
      }
      foo should have size 1
      baz should have size 1
  }

  test("a dataset whose snapshots cannot be listed does not stop the others and is reported at the end") {
    for
      e <- env
      _ <- e.zfs.failingListSnapshots.set(Set("pool/foo/bar"))
      result <- backup(e, full = true, snapshot = Some("s1")).attempt
      foo <- entries(e, "pool/foo")
      baz <- entries(e, "pool/foo/baz")
    yield
      inside(result.left.value) { case z: Z3SError =>
        z.exitCode should === (1)
        z.getMessage should === ("1 of 3 dataset(s) failed: pool/foo/bar")
      }
      foo should have size 1
      baz should have size 1
  }

  test("recursive status lists the tree, including datasets that only exist on S3") {
    for
      e <- env
      _ <- backup(e, full = true, snapshot = Some("s1"))
      // ローカルで削除したdatasetもS3上のバックアップから一覧に出す
      _ <- e.zfs.datasets.update(_ - "pool/foo/baz")
      table <- e.ops.statusText(refresh = false, json = false, recursive = true)
      json <- e.ops.statusText(refresh = false, json = true, recursive = true)
    yield
      table.linesIterator.filter(_.startsWith("== ")).toList should === (
        List("== pool/foo", "== pool/foo/bar", "== pool/foo/baz")
      )
      json should startWith("""[{"dataset":"pool/foo","snapshots":[{"snapshot":"pool/foo@s1"""")
      json should include(""""dataset":"pool/foo/baz"""")
      json should not include "pool/foobar"
  }

  def restore(e: Env, snapshot: String, ids: Option[NonEmptyList[X25519Identity]] = None) =
    e.ops.restore(RestoreOptions(snapshot, Some("pool/r"), force = false, ids, recursive = true))

  test("recursive restore receives the tree under --target, from parents to children") {
    for
      e <- env
      _ <- backup(e, full = true, snapshot = Some("s1"))
      _ <- restore(e, "s1")
      received <- e.zfs.received.get
      datasets <- e.zfs.datasets.get
    yield
      received.map(_._1) should === (List("pool/r", "pool/r/bar", "pool/r/baz"))
      datasets("pool/r/bar").map(_.guid) should === (List(11L))
  }

  test("datasets without a backup of the snapshot are skipped without failing") {
    for
      e <- env
      _ <- backup(e, full = true, snapshot = Some("s1"))
      _ <- backup(e, snapshot = Some("s2"))
      // pool/foo/bazには@s2がないので飛ばす
      _ <- restore(e, "s2")
      received <- e.zfs.received.get
    yield received.map(_._1) should === (List("pool/r", "pool/r", "pool/r/bar", "pool/r/bar"))
  }

  test("descendants of a dataset that fails to restore are skipped and reported") {
    for
      e <- env
      _ <- backup(e, full = true, snapshot = Some("s1"))
      _ <- e.zfs.failingRecvs.set(Set("pool/r"))
      result <- restore(e, "s1").attempt
      received <- e.zfs.received.get
    yield
      inside(result.left.value) { case z: Z3SError =>
        z.exitCode should === (1)
        z.getMessage should === (
          "1 of 3 dataset(s) failed: pool/foo; skipped because an ancestor failed: pool/foo/bar, pool/foo/baz"
        )
      }
      received should === (Nil)
  }

  test("a failing dataset does not stop its siblings from being restored") {
    for
      e <- env
      _ <- backup(e, full = true, snapshot = Some("s1"))
      _ <- e.zfs.failingRecvs.set(Set("pool/r/bar"))
      result <- restore(e, "s1").attempt
      received <- e.zfs.received.get
    yield
      result.left.value.getMessage should === ("1 of 3 dataset(s) failed: pool/foo/bar")
      received.map(_._1) should === (List("pool/r", "pool/r/baz"))
  }

  test("a missing --identity is reported before anything is received") {
    for
      e <- env
      // 先に受信するpool/fooとpool/foo/barはraw、最後のpool/foo/bazだけをage暗号化する
      _ <- e.zfs.nativeEncrypted.update(_ + "pool/foo")
      id <- X25519Identity.generate
      age = SendMode.Plain(Compression.Zstd(3), Encryption.Age(NonEmptyList.one(id.recipient.encode)))
      _ <- backup(e, age, full = true, snapshot = Some("s1"))
      result <- restore(e, "s1").attempt
      received <- e.zfs.received.get
      _ <- restore(e, "s1", Some(NonEmptyList.one(id)))
      receivedWithId <- e.zfs.received.get
    yield
      result.left.value shouldBe a[Z3SError.Usage]
      received should === (Nil)
      receivedWithId.map(_._1) should === (List("pool/r", "pool/r/bar", "pool/r/baz"))
  }

  test("-r is accepted for backup, restore and status") {
    val common = List("--bucket", "b", "--dataset", "pool/foo")
    def parse(args: List[String]) = Cli.command.parse(args, Map.empty).map(_._3)
    parse("backup" :: "-r" :: common).map { case Cmd.Backup(o) => o.recursive; case _ => false } should === (Right(true))
    parse("status" :: "-r" :: common) should === (Right(Cmd.Status(false, false, true)))
    parse("restore" :: "s1" :: "-r" :: common) should === (Right(Cmd.Restore("s1", None, false, None, true)))
  }
