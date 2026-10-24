package dev.capslock.z3s

import cats.effect.{IO, Ref}

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
      zfs.encrypted.set(Set("pool/foo/bar")).as(Env(zfs, store, Ops(common, zfs, store)))
    }

  def backup(e: Env, mode: SendMode = zstd, full: Boolean = false, snapshot: Option[String] = None) =
    e.ops.backup(BackupOptions(full, snapshot, mode, None, recursive = true))

  def entries(e: Env, dataset: String): IO[List[IndexEntry]] =
    e.store.getWithEtag(s"backups/$dataset/.z3s-index.cbor").map(_.toList.flatMap(o => Cbor.decodeIndex(o._1).toOption.get.entries))

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

  test("-r is accepted for backup and status") {
    val common = List("--bucket", "b", "--dataset", "pool/foo")
    def parse(args: List[String]) = Cli.command.parse(args, Map.empty).map(_._3)
    parse("backup" :: "-r" :: common).map { case Cmd.Backup(o) => o.recursive; case _ => false } should === (Right(true))
    parse("status" :: "-r" :: common) should === (Right(Cmd.Status(false, false, true)))
  }
