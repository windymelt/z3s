package dev.capslock.z3s

class PlannerSuite extends UnitSpec:
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

  val daily1 = snap("daily-1", 101, 10)
  val daily2 = snap("daily-2", 102, 20)
  val hourly1 = snap("hourly-1", 201, 25)
  val daily3 = snap("daily-3", 103, 30)
  val daily4 = snap("daily-4", 104, 40)
  val locals = List(daily1, daily2, hourly1, daily3, daily4)

  test("part size") {
    Planner.partSize(0) should === (5 * MiB)
    Planner.partSize(37500L * MiB) should === (5 * MiB) // 37500 MiB * 1.2 / 9000 = 5 MiBちょうど
    Planner.partSize(37500L * MiB + 1) should === (6 * MiB)
    Planner.partSize(1024L * 1024 * MiB) should === (140 * MiB) // 1 TiB -> 139.8 MiB
    // S3のオブジェクトサイズ上限の5 TiBでも9000パートに収まり、10000パートの上限を超えない
    val est = 5L * 1024 * 1024 * MiB
    est * 1.2 / Planner.partSize(est) should be <= 9000.0
  }

  test("select target") {
    Planner.selectTarget(locals, None, None) should === (Right(daily4))
    Planner.selectTarget(locals, Some("hourly"), None) should === (Right(hourly1))
    Planner.selectTarget(locals, Some("daily"), Some("hourly-1")) should === (Right(hourly1))
    Planner.selectTarget(locals, None, Some("nope")).left.value.exitCode should === (1)
    Planner.selectTarget(locals, Some("weekly"), None).left.value.exitCode should === (1)
  }

  test("full backup sends only the target") {
    Planner.planBackup(locals, None, daily4, Index.empty, full = true, ModeKind.Plain) should === (
      Right(BackupPlan.Steps(List(SendStep(None, daily4))))
    )
  }

  test("incremental starts from the latest snapshot on S3 and sends each pair") {
    val index = Index(1, List(entry(daily1, None), entry(daily2, Some(daily1))))
    Planner.planBackup(locals, Some("daily"), daily4, index, full = false, ModeKind.Plain) should === (
      Right(BackupPlan.Steps(List(SendStep(Some(daily2), daily3), SendStep(Some(daily3), daily4))))
    )
    // prefixがなければhourly-1も連鎖に入る
    Planner.planBackup(locals, None, daily4, index, full = false, ModeKind.Plain) should === (
      Right(BackupPlan.Steps(List(SendStep(Some(daily2), hourly1), SendStep(Some(hourly1), daily3), SendStep(Some(daily3), daily4))))
    )
  }

  test("incremental base is matched by guid, not by name") {
    val renamed = Snapshot("tank/data@daily-2", 999, 20)
    val index = Index(1, List(entry(daily1, None), entry(renamed, Some(daily1))))
    Planner.planBackup(locals, Some("daily"), daily3, index, full = false, ModeKind.Plain) should === (
      Right(BackupPlan.Steps(List(SendStep(Some(daily1), daily2), SendStep(Some(daily2), daily3))))
    )
  }

  test("incremental errors") {
    val index = Index(1, List(entry(daily1, None), entry(daily2, Some(daily1))))
    Planner.planBackup(locals, None, daily2, index, full = false, ModeKind.Plain) should === (Right(BackupPlan.UpToDate(daily2)))
    Planner.planBackup(locals, None, daily2, index, full = true, ModeKind.Plain).left.value.exitCode should === (1)

    val noBase = Planner.planBackup(locals, None, daily4, Index.empty, full = false, ModeKind.Plain)
    noBase.left.value.exitCode should === (1)

    val mismatch = Planner.planBackup(locals, None, daily4, index, full = false, ModeKind.Raw)
    mismatch.left.value.exitCode should === (3)

    val broken = Index(1, List(entry(daily2, Some(daily1))))
    Planner.planBackup(locals, None, daily4, broken, full = false, ModeKind.Plain).left.value.exitCode should === (3)
  }

  test("health") {
    val x = snap("x", 301, 50)
    val y = snap("y", 302, 60)
    val index = Index(
      1,
      List(
        entry(daily1, None),
        entry(daily2, Some(daily1)),
        entry(daily3, Some(daily2), SendMode.Raw), // 親とmodeが異なる
        entry(daily4, Some(daily3), SendMode.Raw), // 親がmode mismatch
        entry(hourly1, Some(snap("gone", 999, 1))),
        entry(x, Some(y)),
        entry(y, Some(x))
      )
    )
    val health = Planner.health(index)
    health(daily1.name) should === (Health.Ok)
    health(daily2.name) should === (Health.Ok)
    health(daily3.name) should === (Health.ModeMismatch)
    health(daily4.name) should === (Health.ParentBroken)
    health(hourly1.name) should === (Health.MissingParent)
    health(x.name) should === (Health.Cycle)
    health(y.name) should === (Health.Cycle)
  }

  test("restore chain and resume point") {
    val index = Index(1, List(entry(daily3, Some(daily2)), entry(daily1, None), entry(daily2, Some(daily1)), entry(daily4, Some(daily3))))
    val chain = Planner.restoreChain(index, daily4.name).value
    chain.map(_.header.snapshot) should === (List(daily1, daily2, daily3, daily4).map(_.name))
    Planner.remainingToReceive(chain, Nil).size should === (4)
    val restored = List(Snapshot("pool/r@daily-1", daily1.guid, 1), Snapshot("pool/r@daily-2", daily2.guid, 2))
    Planner.remainingToReceive(chain, restored).map(_.header.snapshot) should === (List(daily3, daily4).map(_.name))
    Planner.remainingToReceive(chain, restored :+ Snapshot("pool/r@x", daily4.guid, 3)) should === (Nil)

    Planner.restoreChain(Index(1, List(entry(daily2, Some(daily1)))), daily2.name).left.value.exitCode should === (3)
    Planner.restoreChain(index, "tank/data@nope").left.value.exitCode should === (1)
  }

  test("health of a long chain is computed without revisiting ancestors") {
    val chain = (1 to 20000).map(i => snap(s"s$i", i.toLong, i.toLong)).toList
    val entries = chain.zip(None :: chain.map(Some(_))).map((s, p) => entry(s, p))
    // 途中の1件の親を欠落させると、それ以降はすべてparent brokenになる
    val broken = entries.updated(10000, entry(chain(10000), Some(snap("gone", 999999, 0))))
    val health = Planner.health(Index(1, broken))
    health(chain(9999).name) should === (Health.Ok)
    health(chain(10000).name) should === (Health.MissingParent)
    health(chain.last.name) should === (Health.ParentBroken)
    Planner.restoreChain(Index(1, entries), chain.last.name).map(_.size) should === (Right(20000))
  }
