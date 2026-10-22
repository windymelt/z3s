package dev.capslock.z3s

import scala.annotation.tailrec
import scala.collection.mutable

/** 1つのバックアップオブジェクトとして送る差分。parentがNoneならfull */
final case class SendStep(parent: Option[Snapshot], child: Snapshot)

enum BackupPlan:
  case Steps(steps: List[SendStep])
  case UpToDate(snapshot: Snapshot)

/** S3とzfsにアクセスせずに行う判断をまとめる */
object Planner:
  private val MiB = 1024L * 1024L
  val MinPartSize: Long = 5 * MiB

  def partSize(estimatedSize: Long): Long =
    val bySize = math.ceil(estimatedSize.toDouble * 1.2 / 9000).toLong
    val size = math.max(MinPartSize, bySize)
    (size + MiB - 1) / MiB * MiB

  def selectTarget(
      locals: List[Snapshot],
      snapshotPrefix: Option[String],
      requested: Option[String]
  ): Either[Z3SError, Snapshot] =
    requested match
      case Some(name) =>
        locals.find(_.shortName == name).toRight(Z3SError.Runtime(s"snapshot not found: $name"))
      case None =>
        filterPrefix(locals, snapshotPrefix).lastOption
          .toRight(Z3SError.Runtime(s"no local snapshot matches prefix ${snapshotPrefix.getOrElse("")}"))

  def filterPrefix(locals: List[Snapshot], snapshotPrefix: Option[String]): List[Snapshot] =
    snapshotPrefix.fold(locals)(p => locals.filter(_.shortName.startsWith(p)))

  /** entryごとの健全性。キーはオブジェクトのキー */
  def health(index: Index): Map[String, Health] =
    val byGuid = index.entries.groupBy(_.header.guid).view.mapValues(_.head).toMap
    val memo = mutable.Map.empty[String, Health]

    // 子の健全性は親の判定結果から決まる
    def fromParent(parent: Health): Health = parent match
      case Health.Ok    => Health.Ok
      case Health.Cycle => Health.Cycle
      case _            => Health.ParentBroken

    // 判定済みのentry、full、または異常のあるentryに着くまで親を辿り、通った経路に結果を書き戻す
    def resolve(start: IndexEntry): Unit =
      val path = mutable.ArrayBuffer.empty[IndexEntry]
      val onPath = mutable.Set.empty[Long]
      var current = start
      var last: Option[Health] = None
      while last.isEmpty do
        memo.get(current.key) match
          case Some(h) => last = Some(fromParent(h))
          case None =>
            path += current
            onPath += current.header.guid
            current.header.parent match
              case None                               => last = Some(Health.Ok)
              case Some(p) if onPath.contains(p.guid) => last = Some(Health.Cycle)
              case Some(p) =>
                byGuid.get(p.guid) match
                  case None                                                        => last = Some(Health.MissingParent)
                  case Some(pe) if pe.header.mode.kind != current.header.mode.kind => last = Some(Health.ModeMismatch)
                  case Some(pe)                                                    => current = pe
      // memoに当たった場合、lastは経路の末尾のentryの健全性になっている
      var h = last.get
      path.reverseIterator.foreach { e =>
        memo(e.key) = h
        h = fromParent(h)
      }

    index.entries.foreach(e => if !memo.contains(e.key) then resolve(e))
    memo.toMap

  def planBackup(
      locals: List[Snapshot],
      snapshotPrefix: Option[String],
      target: Snapshot,
      index: Index,
      full: Boolean,
      mode: ModeKind
  ): Either[Z3SError, BackupPlan] =
    val onS3 = index.entries.map(e => e.header.guid -> e).toMap
    if onS3.contains(target.guid) then
      if full then Left(Z3SError.Runtime(s"${target.name} is already backed up"))
      else Right(BackupPlan.UpToDate(target))
    else if full then Right(BackupPlan.Steps(List(SendStep(None, target))))
    else
      val candidates = filterPrefix(locals, snapshotPrefix).filter(_.createtxg < target.createtxg)
      candidates.reverse.find(s => onS3.contains(s.guid)) match
        case None =>
          Left(Z3SError.Runtime(s"no base snapshot for ${target.name} exists on S3; use --full"))
        case Some(base) =>
          val entry = onS3(base.guid)
          val h = health(index)(entry.key)
          if h != Health.Ok then
            Left(Z3SError.Health(s"base ${base.name} is not restorable: ${h.label}"))
          else if entry.header.mode.kind != mode then
            Left(
              Z3SError.Health(
                s"base ${base.name} was sent in ${entry.header.mode.kind.label} mode, but ${mode.label} was requested"
              )
            )
          else
            val chain = base :: candidates.filter(_.createtxg > base.createtxg) ::: List(target)
            Right(BackupPlan.Steps(chain.zip(chain.tail).map((p, c) => SendStep(Some(p), c))))

  /** fullから対象までのentryを古い順に返す */
  def restoreChain(index: Index, snapshot: String): Either[Z3SError, List[IndexEntry]] =
    val byGuid = index.entries.groupBy(_.header.guid).view.mapValues(_.head).toMap

    @tailrec def loop(e: IndexEntry, acc: List[IndexEntry], visited: Set[Long]): Either[Z3SError, List[IndexEntry]] =
      e.header.parent match
        case None => Right(e :: acc)
        case Some(p) if visited.contains(p.guid) =>
          Left(Z3SError.Health(s"cycle detected at ${e.header.snapshot}"))
        case Some(p) =>
          byGuid.get(p.guid) match
            case None => Left(Z3SError.Health(s"parent ${p.name} of ${e.header.snapshot} is missing on S3"))
            case Some(pe) if pe.header.mode.kind != e.header.mode.kind =>
              Left(Z3SError.Health(s"mode mismatch between ${pe.header.snapshot} and ${e.header.snapshot}"))
            case Some(pe) => loop(pe, e :: acc, visited + p.guid)

    index.entries.find(_.header.snapshot == snapshot) match
      case None    => Left(Z3SError.Runtime(s"$snapshot is not found in the index"))
      case Some(e) => loop(e, Nil, Set(e.header.guid))

  /** 復元先に同じguidのスナップショットがあれば、その次から受信する */
  def remainingToReceive(chain: List[IndexEntry], targetLocals: List[Snapshot]): List[IndexEntry] =
    val localGuids = targetLocals.map(_.guid).toSet
    val last = chain.lastIndexWhere(e => localGuids.contains(e.header.guid))
    chain.drop(last + 1)
