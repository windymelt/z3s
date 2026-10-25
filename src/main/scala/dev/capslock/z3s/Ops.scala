package dev.capslock.z3s

import cats.data.NonEmptyList
import cats.effect.{Clock, IO}
import scala.concurrent.duration.*
import cats.syntax.all.*
import fs2.{Chunk, Pipe, Pull, Stream}
import io.bullet.borer.{Json, Writer}
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Instant
import java.time.format.DateTimeFormatter
import scodec.bits.ByteVector
import dev.capslock.z3s.age.{Age, X25519Identity, X25519Recipient}

val IndexFileName = ".z3s-index.cbor"

final case class Common(
    bucket: String,
    prefix: String,
    dataset: String,
    snapshotPrefix: Option[String],
    dryRun: Boolean,
    concurrency: Int,
    progress: ProgressMode = ProgressMode.None
):
  def objectKey(snapshotName: String): String = s"$prefix$snapshotName"
  def objectPrefix: String = s"$prefix$dataset@"
  def indexKey: String = s"$prefix$dataset/$IndexFileName"

final case class BackupOptions(
    full: Boolean,
    snapshot: Option[String],
    mode: SendMode,
    storageClass: Option[String],
    noResume: Boolean = false,
    /** 再開用の状態の保存先。Noneなら中断時にuploadをabortする */
    stateDir: Option[Path] = None,
    /** 子孫のdatasetもそれぞれバックアップする */
    recursive: Boolean = false
):
  /** age暗号化はfile keyを保存しないと再開できないため、再開の対象にしない */
  def resumable: Boolean = stateDir.isDefined && (mode match
    case SendMode.Raw                         => true
    case SendMode.Plain(_, Encryption.None) => true
    case SendMode.Plain(_, Encryption.Age(_)) => false)

final case class RestoreOptions(
    snapshot: String,
    target: Option[String],
    force: Boolean,
    identities: Option[NonEmptyList[X25519Identity]],
    recursive: Boolean = false
)

final class Ops(common: Common, zfs: Zfs, store: ObjectStore, uploadRetryBase: FiniteDuration = 1.second):
  private val indexStore = IndexStore(store, common.indexKey)
  private val progress = Progress(ProgressMode.resolve(common.progress))
  private val uploader = MultipartUploader(store, common.concurrency, uploadRetryBase, progress.log)
  private def out(s: String): IO[Unit] = IO.println(s)
  private def err(s: String): IO[Unit] = IO.consoleForIO.errorln(s)
  private def now: IO[Instant] = Clock[IO].realTimeInstant

  private def sha256(bytes: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(bytes)

  // ---- reindex ----

  def reindex: IO[Index] =
    val scan = store
      .list(common.objectPrefix)
      .parEvalMap(common.concurrency)(readEntry)
      .unNone
      .compile
      .toList
      .map(entries => Index(Index.SchemaVersion, entries.sortBy(_.header.createtxg)))
    if common.dryRun then
      scan.flatTap(i => out(s"[dry-run] would write index with ${i.entries.size} entries to ${common.indexKey}"))
    else indexStore.replace(scan)

  private def readEntry(o: ObjectInfo): IO[Option[IndexEntry]] =
    store.getPrefix(o.key, Frame.MaxFrameLength).flatMap { bytes =>
      val decoded =
        Frame.codec.decode(ByteVector(bytes).bits).toEither.leftMap(_.messageWithContext).flatMap { r =>
          val frame = r.value
          if frame.version != Frame.CurrentVersion then Left(s"unsupported format version ${frame.version}")
          else Cbor.decodeHeader(frame.header.toArray)
        }
      decoded match
        case Right(h) => IO.pure(Some(IndexEntry(o.key, h, o.size, o.etag, o.lastModified.toString)))
        case Left(e)  => err(s"warning: skipping ${o.key}: $e").as(None)
    }

  private def loadIndex: IO[Index] =
    indexStore.load.flatMap {
      case Some((index, _)) => IO.pure(index)
      case None             => err(s"index ${common.indexKey} not found; running reindex") *> reindex
    }

  // ---- backup ----

  def backup(opts: BackupOptions): IO[Unit] =
    val resumeNotice = opts.stateDir match
      case Some(dir) if opts.resumable => out(s"resume state directory: $dir")
      case Some(dir) if opts.recursive =>
        out(s"resume state directory: $dir (not used for age-encrypted backups)")
      case Some(_) => out("resume: not available for age-encrypted backups")
      case None    => IO.unit

    resumeNotice *> (if opts.recursive then backupTree(opts) else backupDataset(opts, autoFull = false))

  /** 子孫のdatasetを親から順にバックアップする。失敗したdatasetがあっても残りを続け、最後にまとめて報告する */
  private def backupTree(opts: BackupOptions): IO[Unit] =
    for
      datasets <- zfs.listDatasets(common.dataset)
      rootLocals <- zfs.listSnapshots(common.dataset)
      // zfs snapshot -rで作った同じ時点のスナップショットを揃えるため、送る名前はrootで決める
      target <- Planner.selectTarget(rootLocals, common.snapshotPrefix, opts.snapshot).liftTo[IO]
      _ <- out(s"recursive backup of ${datasets.size} dataset(s) at @${target.shortName}")
      results <- datasets.zipWithIndex.traverse { (ds, i) =>
        backupTreeMember(ds, s"[${i + 1}/${datasets.size}]", target.shortName, opts)
      }
      _ <- raiseTreeFailures(results.flatten, Nil, datasets.size)
    yield ()

  /** 木の処理で失敗したdatasetをまとめて1つのエラーにする。終了コードは、失敗の終了コードがすべて同じならそれを、異なれば1を使う */
  private def raiseTreeFailures(failed: List[(String, Throwable)], skipped: List[String], total: Int): IO[Unit] =
    val code = failed.map(_._2).map { case z: Z3SError => z.exitCode; case _ => 1 }.distinct match
      case List(c) => c
      case _       => 1
    val skippedNote =
      if skipped.isEmpty then "" else s"; skipped because an ancestor failed: ${skipped.mkString(", ")}"
    val message = s"${failed.size} of $total dataset(s) failed: ${failed.map(_._1).mkString(", ")}$skippedNote"

    IO.raiseError(Z3SError.fromExitCode(code, message)).whenA(failed.nonEmpty)

  /** 木の中の1つのdatasetをバックアップする。失敗した場合はdataset名とエラーを返す */
  private def backupTreeMember(
      ds: DatasetInfo,
      position: String,
      snapshot: String,
      opts: BackupOptions
  ): IO[Option[(String, Throwable)]] =
    // ZFSネイティブ暗号化されたdatasetは、平文で送らないようにrawにする
    val mode = if ds.nativeEncrypted then SendMode.Raw else opts.mode
    val note = if ds.nativeEncrypted && opts.mode != SendMode.Raw then " (encrypted; sending raw)" else ""
    val child = Ops(common.copy(dataset = ds.name), zfs, store, uploadRetryBase)
    val childOpts = opts.copy(snapshot = Some(snapshot), mode = mode, recursive = false)
    val run = zfs.listSnapshots(ds.name).flatMap { locals =>
      if !locals.exists(_.shortName == snapshot) then err(s"warning: ${ds.name} has no snapshot @$snapshot; skipping")
      else child.backupDataset(childOpts, autoFull = true)
    }

    out(s"== $position ${ds.name}$note") *> run.attempt.flatMap {
      case Right(()) => IO.pure(None)
      case Left(e)   => err(s"error: ${ds.name}: ${e.getMessage}").as(Some((ds.name, e)))
    }

  /** @param autoFull trueなら、S3にバックアップが1つもないdatasetをfullで送る */
  private def backupDataset(opts: BackupOptions, autoFull: Boolean): IO[Unit] =
    for
      locals <- zfs.listSnapshots(common.dataset)
      target <- Planner.selectTarget(locals, common.snapshotPrefix, opts.snapshot).liftTo[IO]
      index <- loadIndex
      full = opts.full || (autoFull && index.entries.isEmpty)
      _ <- out(s"no backups of ${common.dataset} on S3; sending a full stream").whenA(full && !opts.full)
      plan <- Planner
        .planBackup(locals, common.snapshotPrefix, target, index, full, opts.mode.kind)
        .liftTo[IO]
      _ <- plan match
        case BackupPlan.UpToDate(s) => out(s"${s.name} is already backed up")
        case BackupPlan.Steps(steps) =>
          out(s"plan: ${steps.size} object(s) in ${opts.mode.kind.label} mode") *>
            steps.zipWithIndex.traverse_((s, i) => sendOne(s, opts, s"[${i + 1}/${steps.size}]"))
    yield ()

  private def sendOne(step: SendStep, opts: BackupOptions, position: String): IO[Unit] =
    val raw = opts.mode == SendMode.Raw
    val parentName = step.parent.map(_.name)
    val key = common.objectKey(step.child.name)
    for
      estimated <- zfs.estimateSize(step.child.name, parentName, raw)
      partSize = Planner.partSize(estimated)
      createdAt <- now.map(DateTimeFormatter.ISO_INSTANT.format)
      header = Header(
        snapshot = step.child.name,
        guid = step.child.guid,
        createtxg = step.child.createtxg,
        parent = step.parent.map(p => ParentRef(p.name, p.guid)),
        mode = opts.mode,
        estimatedSize = estimated,
        createdAt = createdAt,
        toolVersion = BuildInfo.version
      )
      _ <- out(
        s"${parentName.fold("full")(p => s"incremental from $p")} -> ${step.child.name}: " +
          s"estimated $estimated bytes, part size $partSize, key $key"
      )
      _ <- out(s"  ${ZfsCommands.show(ZfsCommands.send(step.child.name, parentName, raw))}")
      _ <-
        if common.dryRun then IO.unit
        else upload(key, header, parentName, partSize, opts, s"$position ${step.child.name}")
    yield ()

  private def upload(
      key: String,
      header: Header,
      parentName: Option[String],
      partSize: Long,
      opts: BackupOptions,
      label: String
  ): IO[Unit] =
    val states = opts.stateDir.filter(_ => opts.resumable).map(StateStore(_, progress.log))
    val metadata = Map("z3s-format" -> Frame.CurrentVersion.toString) ++ parentName.map("z3s-parent" -> _)
    val raw = opts.mode == SendMode.Raw
    for
      _ <- store.head(key).flatMap {
        case Some(_) => IO.raiseError(Z3SError.Runtime(s"object already exists: $key"))
        case None    => IO.unit
      }
      resume <- states.flatTraverse(prepareResume(_, key, header, opts.noResume))
      // 再開する場合は、中断前のヘッダ(createdAtを含む)とパートサイズを使って同じバイト列を作る
      (headerBytes, effectiveHeader, effectivePartSize) <- resume match
        case Some(r) =>
          Cbor.decodeHeader(r.state.header.toArray).leftMap(Z3SError.Runtime(_)).liftTo[IO].map { h =>
            (r.state.header.toArray, h, r.state.partSize)
          }
        case None => IO.pure((Cbor.encodeHeader(header), header, partSize.toInt))
      _ <- IO
        .raiseError(Z3SError.Runtime(s"header is too large: ${headerBytes.length} bytes"))
        .whenA(headerBytes.length > Frame.MaxHeaderLength)
      frame = Frame.encode(Frame(Frame.CurrentVersion, ByteVector(headerBytes)))
      counters <- PartCounters.make
      // 見積もりはzfs sendの出力サイズなので、圧縮・暗号化の前のバイト数で進捗を数える
      send = zfs
        .send(header.snapshot, parentName, raw)
        .through(progress.track(label, Some(effectiveHeader.estimatedSize), counters.describe))
      payload <- opts.mode match
        case SendMode.Raw => IO.pure(send)
        case SendMode.Plain(c, e) =>
          val compressed = send.through(Compressors.compress(c))
          e match
            case Encryption.None => IO.pure(compressed)
            case Encryption.Age(rs) =>
              rs.traverse(X25519Recipient.parse)
                .leftMap(Z3SError.Usage(_))
                .liftTo[IO]
                .map(recipients =>
                  (Stream.chunk(Chunk.array(sha256(headerBytes))) ++ compressed).through(Age.encrypt(recipients))
                )
      result <- uploader.upload(
        common.bucket,
        key,
        metadata,
        opts.storageClass,
        effectivePartSize,
        ByteVector(headerBytes),
        Stream.chunk(Chunk.byteVector(frame)) ++ payload,
        states,
        resume,
        counters
      )
      uploadedAt <- now
      _ <- out(s"uploaded $key (${result.size} bytes)")
      entry = IndexEntry(key, effectiveHeader, result.size, result.etag, uploadedAt.toString)
      _ <- indexStore.update(_.upsert(entry)).void.adaptError { case e =>
        Z3SError.Runtime(s"$key was uploaded, but updating the index failed (${e.getMessage}); run `z3s reindex`", e)
      }
    yield ()

  /** 中断したuploadの状態があり、同じスナップショットとmodeなら再開する。そうでなければ古いuploadをabortする */
  private def prepareResume(states: StateStore, key: String, header: Header, noResume: Boolean): IO[Option[Resume]] =
    states.load(common.bucket, key).flatMap {
      case None => IO.pure(None)
      case Some(state) =>
        val mismatch: Option[String] =
          if noResume then Some("--no-resume was given")
          else
            Cbor.decodeHeader(state.header.toArray) match
              case Left(e) => Some(e)
              case Right(h) if h.guid != header.guid || h.parent != header.parent || h.mode != header.mode =>
                Some("the snapshot or the mode differs")
              case Right(_) => None
        mismatch match
          case Some(reason) =>
            progress.log(s"discarding interrupted upload ${state.uploadId} of $key ($reason)") *>
              store
                .abortMultipartUpload(key, state.uploadId)
                .handleErrorWith(e => progress.log(s"warning: failed to abort ${state.uploadId}: ${e.getMessage}")) *>
              states.delete(common.bucket, key).as(None)
          case None =>
            store.listParts(key, state.uploadId).flatMap {
              case None =>
                progress.log(s"interrupted upload ${state.uploadId} of $key no longer exists; starting over") *>
                  states.delete(common.bucket, key).as(None)
              case Some(parts) =>
                progress
                  .log(
                    s"resuming upload ${state.uploadId} of $key: " +
                      s"${parts.size} parts on S3, ${state.parts.size} parts recorded"
                  )
                  .as(Some(Resume(state, parts)))
            }
    }

  // ---- restore ----

  def restore(opts: RestoreOptions): IO[Unit] =
    if opts.recursive then restoreTree(opts) else restoreDataset(opts)

  private def restoreDataset(opts: RestoreOptions): IO[Unit] =
    val target = opts.target.getOrElse(common.dataset)
    for
      index <- loadIndex
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
      datasets <- remoteTreeDatasets
      _ <- IO.raiseError(Z3SError.Runtime(s"no backups of $root or its descendants on S3")).whenA(datasets.isEmpty)
      // 受信を始める前に、全datasetの計画を立てて--identityの要否を確かめる
      plans <- datasets.traverse { ds =>
        Ops(common.copy(dataset = ds), zfs, store, uploadRetryBase).planTreeMember(targetOf(ds), opts.snapshot)
      }
      toReceive = plans.flatMap(_.plan.toOption.flatten).flatten
      _ <- requireIdentity(opts, toReceive)
      _ <- out(s"recursive restore of ${datasets.size} dataset(s) at @${opts.snapshot} into $targetRoot")
      result <- plans.zipWithIndex.foldLeftM(TreeRestore.empty) { case (acc, (plan, i)) =>
        restoreTreeStep(acc, plan, s"[${i + 1}/${datasets.size}]", opts)
      }
      _ <- raiseTreeFailures(result.failed, result.skipped, datasets.size)
    yield ()

  /** このdatasetの計画を立てる。snapshotのバックアップがなければNone、計画を立てられなければLeftにする */
  private def planTreeMember(target: String, snapshot: String): IO[TreeMemberPlan] =
    val ds = common.dataset
    loadIndex
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
    else p.ops.restoreTreeMember(p.plan, position, p.target, opts).map(_.fold(acc)(acc.fail(p.dataset, _)))

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
            store.get(entry.key).through(progress.track(s"$position ${entry.header.snapshot}", Some(entry.size)))
          )(decodePayload(entry, opts))
        )

  private def decodePayload(entry: IndexEntry, opts: RestoreOptions)(
      frame: Frame,
      payload: Stream[IO, Byte]
  ): Stream[IO, Byte] =
    val checked =
      if frame.version != Frame.CurrentVersion then Left(s"unsupported format version ${frame.version}")
      else
        Cbor.decodeHeader(frame.header.toArray).flatMap { h =>
          Either.cond(h == entry.header, h, s"header of ${entry.key} does not match the index; run `z3s reindex`")
        }
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
              .through(verifyHeaderDigest(sha256(frame.header.toArray), entry.key))
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

  // ---- status ----

  def status(refresh: Boolean, json: Boolean, recursive: Boolean = false): IO[Unit] =
    statusText(refresh, json, recursive).flatMap(out)

  def statusText(refresh: Boolean, json: Boolean, recursive: Boolean): IO[String] =
    if !recursive then statusRows(refresh).map(rows => if json then StatusRow.json(rows) else StatusRow.table(rows))
    else
      for
        datasets <- treeDatasets
        sections <- datasets.traverse(ds => Ops(common.copy(dataset = ds), zfs, store).statusRows(refresh).map(ds -> _))
      yield if json then StatusRow.jsonTree(sections) else StatusRow.tableTree(sections)

  private def statusRows(refresh: Boolean): IO[List[StatusRow]] =
    for
      index <- if refresh then reindex else loadIndex
      exists <- zfs.datasetExists(common.dataset)
      locals <- if exists then zfs.listSnapshots(common.dataset) else IO.pure(Nil)
      health = Planner.health(index)
      localGuids = locals.map(_.guid).toSet
    yield index.entries.sortBy(_.header.createtxg).map { e =>
      StatusRow(e, health(e.key), localGuids.contains(e.header.guid))
    }

  /** ローカルの子孫と、S3にバックアップかインデックスがあるdatasetを合わせて、名前の順に返す */
  private def treeDatasets: IO[List[String]] =
    for
      exists <- zfs.datasetExists(common.dataset)
      local <- if exists then zfs.listDatasets(common.dataset).map(_.map(_.name)) else IO.pure(Nil)
      remote <- remoteTreeDatasets
    yield (local ++ remote).distinct.sorted

  /** rootとその子孫のうち、S3にバックアップかインデックスがあるdatasetを名前の順(親が子より先)に返す */
  private def remoteTreeDatasets: IO[List[String]] =
    val root = common.dataset
    def isInTree(name: String) = name == root || name.startsWith(s"$root/")

    // prefixの一覧には兄弟のdataset(pool/foobarなど)も含まれるため、isInTreeで絞る
    store
      .list(s"${common.prefix}$root")
      .map(_.key.stripPrefix(common.prefix))
      // インデックスのキー(<dataset>/.z3s-index.cbor)とバックアップのキー(<dataset>@<snapshot>)から、datasetの名前を取り出す
      .collect {
        case k if k.endsWith(s"/$IndexFileName") => k.stripSuffix(s"/$IndexFileName")
        case k if k.contains('@')                => k.takeWhile(_ != '@')
      }
      .filter(isInTree)
      .compile
      .toList
      .map(_.distinct.sorted)

/** restore --recursiveで、1つのdatasetについて立てた計画 */
private[z3s] final case class TreeMemberPlan(
    ops: Ops,
    dataset: String,
    target: String,
    plan: Either[Throwable, Option[List[IndexEntry]]]
)

/** restore --recursiveで、失敗したdatasetと、先祖の失敗により飛ばしたdataset */
private[z3s] final case class TreeRestore(failed: List[(String, Throwable)], skipped: List[String]):
  def hasFailedAncestor(dataset: String): Boolean =
    (failed.map(_._1) ++ skipped).exists(a => dataset.startsWith(s"$a/"))
  def fail(dataset: String, e: Throwable): TreeRestore = copy(failed = failed :+ (dataset -> e))
  def skip(dataset: String): TreeRestore = copy(skipped = skipped :+ dataset)

private[z3s] object TreeRestore:
  val empty: TreeRestore = TreeRestore(Nil, Nil)

final case class StatusRow(entry: IndexEntry, health: Health, local: Boolean):
  def h: Header = entry.header
  def kind: String = if h.isFull then "full" else "incremental"
  def mode: String = h.mode match
    case SendMode.Raw         => "raw"
    case SendMode.Plain(c, e) => s"plain(${c.label}, ${e.label})"

object StatusRow:
  def table(rows: List[StatusRow]): String =
    val header = List("SNAPSHOT", "PARENT", "KIND", "MODE", "HEALTH", "LOCAL", "ESTIMATED", "SIZE")
    val body = rows.map(r =>
      List(
        r.h.snapshot,
        r.h.parent.fold("-")(_.name),
        r.kind,
        r.mode,
        r.health.label,
        if r.local then "yes" else "no",
        r.h.estimatedSize.toString,
        r.entry.size.toString
      )
    )
    val all = header :: body
    val widths = header.indices.map(i => all.map(_(i).length).max)
    all.map(_.zip(widths).map((s, w) => s.padTo(w, ' ')).mkString("  ").stripTrailing).mkString("\n")

  def tableTree(sections: List[(String, List[StatusRow])]): String =
    sections
      .map((dataset, rows) => s"== $dataset\n" + (if rows.isEmpty then "(no backups)" else table(rows)))
      .mkString("\n\n")

  def jsonTree(sections: List[(String, List[StatusRow])]): String =
    sections
      .map((dataset, rows) => s"""{"dataset":${Json.encode(dataset).toUtf8String},"snapshots":${json(rows)}}""")
      .mkString("[", ",", "]")

  def json(rows: List[StatusRow]): String =
    def row(w: Writer, r: StatusRow): Writer =
      w.writeMapStart()
      w.writeString("snapshot").writeString(r.h.snapshot)
      w.writeString("guid").writeString(showGuid(r.h.guid))
      w.writeString("parent")
      r.h.parent.fold(w.writeNull())(p => w.writeString(p.name))
      w.writeString("kind").writeString(r.kind)
      w.writeString("mode").writeString(r.h.mode.kind.label)
      w.writeString("compression")
      r.h.mode match
        case SendMode.Plain(c, _) => w.writeString(c.label)
        case SendMode.Raw         => w.writeNull()
      w.writeString("encryption")
      r.h.mode match
        case SendMode.Plain(_, e) => w.writeString(e match { case Encryption.None => "none"; case _ => "age" })
        case SendMode.Raw         => w.writeNull()
      w.writeString("health").writeString(r.health.label)
      w.writeString("local").writeBoolean(r.local)
      w.writeString("estimatedSize").writeLong(r.h.estimatedSize)
      w.writeString("objectSize").writeLong(r.entry.size)
      w.writeBreak()

    given io.bullet.borer.Encoder[List[StatusRow]] = io.bullet.borer.Encoder { (w, rs) =>
      // borerのJSON出力は長さ付きのarrayとmapに対応しないため、不定長で書く
      w.writeArrayStart()
      rs.foreach(row(w, _))
      w.writeBreak()
    }
    Json.encode(rows).toUtf8String
