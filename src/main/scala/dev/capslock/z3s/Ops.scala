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
    identities: Option[NonEmptyList[X25519Identity]]
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
      failed = results.flatten
      _ <- IO.raiseError(Z3SError.fromExitCode(
        failed.map(_._2).map { case z: Z3SError => z.exitCode; case _ => 1 }.distinct match
          case List(code) => code
          case _          => 1,
        s"${failed.size} of ${datasets.size} dataset(s) failed: ${failed.map(_._1).mkString(", ")}"
      )).whenA(failed.nonEmpty)
    yield ()

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
    out(s"== $position ${ds.name}$note") *>
      zfs.listSnapshots(ds.name).flatMap { locals =>
        if !locals.exists(_.shortName == snapshot) then
          err(s"warning: ${ds.name} has no snapshot @$snapshot; skipping").as(None)
        else
          child.backupDataset(childOpts, autoFull = true).attempt.flatMap {
            case Right(()) => IO.pure(None)
            case Left(e)   => err(s"error: ${ds.name}: ${e.getMessage}").as(Some((ds.name, e)))
          }
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
    val target = opts.target.getOrElse(common.dataset)
    val snapshotName = s"${common.dataset}@${opts.snapshot}"
    for
      index <- loadIndex
      chain <- Planner.restoreChain(index, snapshotName).liftTo[IO]
      exists <- zfs.datasetExists(target)
      targetLocals <- if exists then zfs.listSnapshots(target) else IO.pure(Nil)
      remaining = Planner.remainingToReceive(chain, targetLocals)
      _ <- IO
        .raiseError(Z3SError.Usage("--identity is required to restore age-encrypted objects"))
        .whenA(opts.identities.isEmpty && remaining.exists(e => isAge(e.header.mode)))
      _ <-
        if remaining.isEmpty then out(s"$target already has $snapshotName")
        else
          out(s"plan: receive ${remaining.size} object(s) into $target") *>
            remaining.zipWithIndex.traverse_((e, i) => receiveOne(e, target, opts, s"[${i + 1}/${remaining.size}]"))
    yield ()

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
    val root = common.dataset
    def isInTree(name: String) = name == root || name.startsWith(s"$root/")
    for
      exists <- zfs.datasetExists(root)
      local <- if exists then zfs.listDatasets(root).map(_.map(_.name)) else IO.pure(Nil)
      // prefixの一覧には兄弟のdataset(pool/foobarなど)も含まれるため、isInTreeで絞る
      remote <- store
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
    yield (local ++ remote).distinct.sorted

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
