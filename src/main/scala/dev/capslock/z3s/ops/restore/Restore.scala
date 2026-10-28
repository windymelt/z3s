package dev.capslock.z3s.ops.restore

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import dev.capslock.z3s.*
import dev.capslock.z3s.age.{Age, X25519Identity}
import dev.capslock.z3s.codec.header.HeaderCodec
import dev.capslock.z3s.ops.{OpsContext, Tree}
import dev.capslock.z3s.ops.reindex.Reindex
import fs2.{Pipe, Pull, Stream}

final case class RestoreOptions(
    snapshot: String,
    target: Option[String],
    force: Boolean,
    identities: Option[NonEmptyList[X25519Identity]],
    recursive: Boolean = false
)

/** restoreサブコマンドを処理する。S3のバックアップを古いものから順にzfs recvで受信し、指定したスナップショットを復元する */
private[ops] final class Restore(ctx: OpsContext):
  import ctx.{common, err, out, store, zfs}

  def restore(opts: RestoreOptions): IO[Unit] =
    if opts.recursive then restoreTree(opts) else restoreDataset(opts)

  private def restoreDataset(opts: RestoreOptions): IO[Unit] =
    val target = opts.target.getOrElse(common.dataset)
    for
      index <- Reindex(ctx).loadOrCreateIndex
      remaining <- planRestore(index, target, opts.snapshot)
      _ <- requireIdentity(opts, remaining)
      _ <- receiveAll(remaining, target, opts)
    yield ()

  /** S3にあるdatasetの木を親から順にリストアする。受信先は--targetの下に、rootからの相対パスで作る */
  private def restoreTree(opts: RestoreOptions): IO[Unit] =
    val root = common.dataset
    val targetRoot = opts.target.getOrElse(root)
    def targetOf(ds: String) = targetRoot + ds.stripPrefix(root)

    for
      datasets <- Tree.remoteDatasets(ctx)
      _ <- IO.raiseError(Z3SError.Runtime(s"no backups of $root or its descendants on S3")).whenA(datasets.isEmpty)
      // 受信を始める前に、全datasetの計画を立てて--identityの要否を確かめる
      plans <- datasets.traverse(ds => Restore(ctx.forDataset(ds)).planTreeMember(targetOf(ds), opts.snapshot))
      toReceive = plans.flatMap(_.plan.toOption.flatten).flatten
      _ <- requireIdentity(opts, toReceive)
      _ <- out(s"recursive restore of ${datasets.size} dataset(s) at @${opts.snapshot} into $targetRoot")
      result <- plans.zipWithIndex.foldLeftM(TreeRestore.empty) { case (acc, (plan, i)) =>
        restoreTreeStep(acc, plan, s"[${i + 1}/${datasets.size}]", opts)
      }
      _ <- Tree.raiseFailures(result.failed, result.skipped, datasets.size)
    yield ()

  /** このdatasetの計画を立てる。snapshotのバックアップがなければNone、計画を立てられなければLeftにする */
  private def planTreeMember(target: String, snapshot: String): IO[TreeMemberPlan] =
    val ds = common.dataset
    Reindex(ctx).loadOrCreateIndex
      .flatMap { index =>
        if !index.entries.exists(_.header.snapshot == s"$ds@$snapshot") then IO.pure(None)
        else planRestore(index, target, snapshot).map(Some(_))
      }
      .attempt
      .map(TreeMemberPlan(this, ds, target, _))

  /** 1つのdatasetを受信し、結果をaccに加える。先祖が失敗していれば受信せずに飛ばす */
  private def restoreTreeStep(
      acc: TreeRestore,
      p: TreeMemberPlan,
      position: String,
      opts: RestoreOptions
  ): IO[TreeRestore] =
    // 親がないとzfs recvは子を作れないため、失敗したdatasetの子孫は受信しない
    if acc.hasFailedAncestor(p.dataset) then out(s"== $position ${p.dataset} (skipped)").as(acc.skip(p.dataset))
    else p.restore.restoreTreeMember(p.plan, position, p.target, opts).map(_.fold(acc)(acc.fail(p.dataset, _)))

  /** 木の中の1つのdatasetを計画どおりに受信する。失敗した場合はエラーを返す */
  private def restoreTreeMember(
      plan: Either[Throwable, Option[List[IndexEntry]]],
      position: String,
      target: String,
      opts: RestoreOptions
  ): IO[Option[Throwable]] =
    val ds = common.dataset
    val run = plan.liftTo[IO].flatMap {
      case None            => err(s"warning: $ds has no backup @${opts.snapshot}; skipping")
      case Some(remaining) => receiveAll(remaining, target, opts)
    }

    out(s"== $position $ds -> $target") *> run.attempt.flatMap {
      case Right(()) => IO.pure(None)
      case Left(e)   => err(s"error: $ds: ${e.getMessage}").as(Some(e))
    }

  /** targetがすでに持っているスナップショットの後から、snapshotまでに受信するオブジェクトを返す */
  private def planRestore(index: Index, target: String, snapshot: String): IO[List[IndexEntry]] =
    for
      chain <- Planner.restoreChain(index, s"${common.dataset}@$snapshot").liftTo[IO]
      exists <- zfs.datasetExists(target)
      targetLocals <- if exists then zfs.listSnapshots(target) else IO.pure(Nil)
    yield Planner.remainingToReceive(chain, targetLocals)

  /** 受信するオブジェクトにage暗号化されたものがあるのに--identityがなければ、引数エラーにする */
  private def requireIdentity(opts: RestoreOptions, remaining: List[IndexEntry]): IO[Unit] =
    IO.raiseError(Z3SError.Usage("--identity is required to restore age-encrypted objects"))
      .whenA(opts.identities.isEmpty && remaining.exists(e => isAge(e.header.mode)))

  private def receiveAll(remaining: List[IndexEntry], target: String, opts: RestoreOptions): IO[Unit] =
    if remaining.isEmpty then out(s"$target already has ${common.dataset}@${opts.snapshot}")
    else
      out(s"plan: receive ${remaining.size} object(s) into $target") *>
        remaining.zipWithIndex.traverse_((e, i) => receiveOne(e, target, opts, s"[${i + 1}/${remaining.size}]"))

  private def isAge(mode: SendMode): Boolean = mode match
    case SendMode.Plain(_, Encryption.Age(_)) => true
    case _                                    => false

  private def receiveOne(entry: IndexEntry, target: String, opts: RestoreOptions, position: String): IO[Unit] =
    val describe = s"${entry.key} | ${ZfsCommands.show(ZfsCommands.recv(target, opts.force))}"
    if common.dryRun then out(s"[dry-run] $describe")
    else
      out(describe) *>
        zfs.recv(
          target,
          opts.force,
          Frame.split(
            store.get(entry.key).through(ctx.progress.track(s"$position ${entry.header.snapshot}", Some(entry.size)))
          )(decodePayload(entry, opts))
        )

  private def decodePayload(entry: IndexEntry, opts: RestoreOptions)(
      frame: Frame,
      payload: Stream[IO, Byte]
  ): Stream[IO, Byte] =
    val checked =
      if frame.version == Frame.CurrentVersion then
        HeaderCodec.decode(frame.header.toArray).flatMap { h =>
          Either.cond(h == entry.header, h, s"header of ${entry.key} does not match the index; run `z3s reindex`")
        }
      else Left(s"unsupported format version ${frame.version}")
    checked match
      case Left(e) => Stream.raiseError[IO](Z3SError.Runtime(e))
      case Right(h) =>
        h.mode match
          case SendMode.Raw                         => payload
          case SendMode.Plain(c, Encryption.None) => payload.through(Compressors.decompress(c))
          case SendMode.Plain(c, Encryption.Age(_)) =>
            val ids = opts.identities.get
            payload
              .through(Age.decrypt(ids))
              .through(verifyHeaderDigest(ctx.sha256(frame.header.toArray), entry.key))
              .through(Compressors.decompress(c))

  /** 復号した平文の先頭32バイトが平文ヘッダのSHA-256と一致することを確かめ、取り除く */
  private def verifyHeaderDigest(expected: Array[Byte], key: String): Pipe[IO, Byte, Byte] = in =>
    in.pull
      .unconsN(32)
      .flatMap {
        case Some((digest, rest)) if java.util.Arrays.equals(digest.toArray, expected) => rest.pull.echo
        case _ => Pull.raiseError[IO](Z3SError.Runtime(s"header digest of $key does not match the encrypted payload"))
      }
      .stream

/** restore --recursiveで、1つのdatasetについて立てた計画 */
private[restore] final case class TreeMemberPlan(
    restore: Restore,
    dataset: String,
    target: String,
    plan: Either[Throwable, Option[List[IndexEntry]]]
)

/** restore --recursiveで、失敗したdatasetと、先祖の失敗により飛ばしたdataset */
private[restore] final case class TreeRestore(failed: List[Tree.Failure], skipped: List[String]):
  def hasFailedAncestor(dataset: String): Boolean =
    (failed.map(_.dataset) ++ skipped).exists(a => dataset.startsWith(s"$a/"))
  def fail(dataset: String, e: Throwable): TreeRestore = copy(failed = failed.appended((dataset = dataset, error = e)))
  def skip(dataset: String): TreeRestore = copy(skipped = skipped :+ dataset)

private[restore] object TreeRestore:
  val empty: TreeRestore = TreeRestore(Nil, Nil)
