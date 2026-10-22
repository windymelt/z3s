package dev.capslock.z3s

import munit.FunSuite

class PlannerSuite extends FunSuite:
  val MiB = 1024L * 1024L
  val plain = SendMode.Plain(Compression.Zstd(3), Encryption.None)

  def snap(short: String, guid: Long, txg: Long) = Snapshot(s"tank/data@$short", guid, txg)

  def entry(s: Snapshot, parent: Option[Snapshot], mode: SendMode = plain) =
    IndexEntry(
      key = s.name,
      header = Header(s.name, s.guid, s.createtxg, parent.map(p => ParentRef(p.name, p.guid)), mode, 0, "", ""),
      size = 0,
      etag = "",
      uploadedAt = ""
    )

  val a = snap("daily-1", 101, 10)
  val b = snap("daily-2", 102, 20)
  val h = snap("hourly-1", 201, 25)
  val c = snap("daily-3", 103, 30)
  val d = snap("daily-4", 104, 40)
  val locals = List(a, b, h, c, d)

  test("part size") {
    assertEquals(Planner.partSize(0), 5 * MiB)
    assertEquals(Planner.partSize(37500L * MiB), 5 * MiB) // 37500 MiB * 1.2 / 9000 = 5 MiBちょうど
    assertEquals(Planner.partSize(37500L * MiB + 1), 6 * MiB)
    assertEquals(Planner.partSize(1024L * 1024 * MiB), 140 * MiB) // 1 TiB -> 139.8 MiB
    // S3のオブジェクトサイズ上限の5 TiBでも9000パートに収まり、10000パートの上限を超えない
    val est = 5L * 1024 * 1024 * MiB
    assert(est * 1.2 / Planner.partSize(est) <= 9000)
  }

  test("select target") {
    assertEquals(Planner.selectTarget(locals, None, None), Right(d))
    assertEquals(Planner.selectTarget(locals, Some("hourly"), None), Right(h))
    assertEquals(Planner.selectTarget(locals, Some("daily"), Some("hourly-1")), Right(h))
    assert(Planner.selectTarget(locals, None, Some("nope")).isLeft)
    assert(Planner.selectTarget(locals, Some("weekly"), None).isLeft)
  }

  test("full backup sends only the target") {
    assertEquals(
      Planner.planBackup(locals, None, d, Index.empty, full = true, ModeKind.Plain),
      Right(BackupPlan.Steps(List(SendStep(None, d))))
    )
  }

  test("incremental starts from the latest snapshot on S3 and sends each pair") {
    val index = Index(1, List(entry(a, None), entry(b, Some(a))))
    assertEquals(
      Planner.planBackup(locals, Some("daily"), d, index, full = false, ModeKind.Plain),
      Right(BackupPlan.Steps(List(SendStep(Some(b), c), SendStep(Some(c), d))))
    )
    // prefixがなければhourly-1も連鎖に入る
    assertEquals(
      Planner.planBackup(locals, None, d, index, full = false, ModeKind.Plain),
      Right(BackupPlan.Steps(List(SendStep(Some(b), h), SendStep(Some(h), c), SendStep(Some(c), d))))
    )
  }

  test("incremental base is matched by guid, not by name") {
    val renamed = Snapshot("tank/data@daily-2", 999, 20)
    val index = Index(1, List(entry(a, None), entry(renamed, Some(a))))
    assertEquals(
      Planner.planBackup(locals, Some("daily"), c, index, full = false, ModeKind.Plain),
      Right(BackupPlan.Steps(List(SendStep(Some(a), b), SendStep(Some(b), c))))
    )
  }

  test("incremental errors") {
    val index = Index(1, List(entry(a, None), entry(b, Some(a))))
    assertEquals(
      Planner.planBackup(locals, None, b, index, full = false, ModeKind.Plain),
      Right(BackupPlan.UpToDate(b))
    )
    assert(Planner.planBackup(locals, None, b, index, full = true, ModeKind.Plain).isLeft)

    val noBase = Planner.planBackup(locals, None, d, Index.empty, full = false, ModeKind.Plain)
    assertEquals(noBase.left.map(_.exitCode), Left(1))

    val mismatch = Planner.planBackup(locals, None, d, index, full = false, ModeKind.Raw)
    assertEquals(mismatch.left.map(_.exitCode), Left(3))

    val broken = Index(1, List(entry(b, Some(a))))
    assertEquals(Planner.planBackup(locals, None, d, broken, full = false, ModeKind.Plain).left.map(_.exitCode), Left(3))
  }

  test("health") {
    val x = snap("x", 301, 50)
    val y = snap("y", 302, 60)
    val index = Index(
      1,
      List(
        entry(a, None),
        entry(b, Some(a)),
        entry(c, Some(b), SendMode.Raw), // 親とmodeが異なる
        entry(d, Some(c), SendMode.Raw), // 親がmode mismatch
        entry(h, Some(snap("gone", 999, 1))),
        entry(x, Some(y)),
        entry(y, Some(x))
      )
    )
    val health = Planner.health(index)
    assertEquals(health(a.name), Health.Ok)
    assertEquals(health(b.name), Health.Ok)
    assertEquals(health(c.name), Health.ModeMismatch)
    assertEquals(health(d.name), Health.ParentBroken)
    assertEquals(health(h.name), Health.MissingParent)
    assertEquals(health(x.name), Health.Cycle)
    assertEquals(health(y.name), Health.Cycle)
  }

  test("restore chain and resume point") {
    val index = Index(1, List(entry(c, Some(b)), entry(a, None), entry(b, Some(a)), entry(d, Some(c))))
    val chain = Planner.restoreChain(index, d.name).toOption.get
    assertEquals(chain.map(_.header.snapshot), List(a, b, c, d).map(_.name))
    assertEquals(Planner.remainingToReceive(chain, Nil).size, 4)
    val restored = List(Snapshot("pool/r@daily-1", a.guid, 1), Snapshot("pool/r@daily-2", b.guid, 2))
    assertEquals(Planner.remainingToReceive(chain, restored).map(_.header.snapshot), List(c, d).map(_.name))
    assertEquals(Planner.remainingToReceive(chain, restored :+ Snapshot("pool/r@x", d.guid, 3)), Nil)

    assertEquals(Planner.restoreChain(Index(1, List(entry(b, Some(a)))), b.name).left.map(_.exitCode), Left(3))
    assertEquals(Planner.restoreChain(index, "tank/data@nope").left.map(_.exitCode), Left(1))
  }

  test("health of a long chain is computed without revisiting ancestors") {
    val chain = (1 to 20000).map(i => snap(s"s$i", i.toLong, i.toLong)).toList
    val entries = chain.zip(None :: chain.map(Some(_))).map((s, p) => entry(s, p))
    // 途中の1件の親を欠落させると、それ以降はすべてparent brokenになる
    val broken = entries.updated(10000, entry(chain(10000), Some(snap("gone", 999999, 0))))
    val health = Planner.health(Index(1, broken))
    assertEquals(health(chain(9999).name), Health.Ok)
    assertEquals(health(chain(10000).name), Health.MissingParent)
    assertEquals(health(chain.last.name), Health.ParentBroken)
    assertEquals(Planner.restoreChain(Index(1, entries), chain.last.name).map(_.size), Right(20000))
  }
