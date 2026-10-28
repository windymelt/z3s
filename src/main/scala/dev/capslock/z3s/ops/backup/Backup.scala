package dev.capslock.z3s.ops.backup

import cats.effect.IO
import cats.syntax.all.*
import dev.capslock.z3s.*
import dev.capslock.z3s.age.{Age, X25519Recipient}
import dev.capslock.z3s.codec.header.HeaderCodec
import dev.capslock.z3s.codec.partdigests.PartDigestsCodec
import dev.capslock.z3s.ops.{OpsContext, Tree}
import dev.capslock.z3s.ops.reindex.Reindex
import fs2.{Chunk, Stream}
import java.nio.file.Path
import java.time.format.DateTimeFormatter
import scodec.bits.ByteVector

final case class BackupOptions(
    full: Boolean,
    snapshot: Option[String],
    mode: SendMode,
    storageClass: Option[String],
    noResume: Boolean = false,
    /** 再開用の状態の保存先。Noneなら中断時にuploadをabortする */
    stateDir: Option[Path] = None,
    recursive: Boolean = false
):
  /** age暗号化はfile keyを保存しないと再開できないため、再開の対象にしない */
  def isResumable: Boolean = stateDir.isDefined && (mode match
    case SendMode.Raw                         => true
    case SendMode.Plain(_, Encryption.None) => true
    case SendMode.Plain(_, Encryption.Age(_)) => false)

  /** --recursiveで木の中の1つのdatasetを送るときの設定を返す。子孫は呼び出し元が1つずつ処理するので、recursiveを外す */
  def forTreeMember(snapshot: String, mode: SendMode): BackupOptions =
    copy(snapshot = Some(snapshot), mode = mode, recursive = false)

/** backupサブコマンドを処理する。スナップショットをzfs sendで送ってS3にアップロードし、インデックスに加える */
private[ops] final class Backup(ctx: OpsContext):
  import ctx.{common, err, now, out, progress, store, zfs}

  def backup(opts: BackupOptions): IO[Unit] =
    val resumeNotice = opts.stateDir match
      case Some(dir) if opts.isResumable => out(s"resume state directory: $dir")
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
      rootSnapshot <- Planner.selectTarget(rootLocals, common.snapshotPrefix, opts.snapshot).liftTo[IO]
      _ <- out(s"recursive backup of ${datasets.size} dataset(s) at @${rootSnapshot.shortName}")
      results <- datasets.zipWithIndex.traverse { (ds, i) =>
        backupTreeMember(ds, s"[${i + 1}/${datasets.size}]", rootSnapshot.shortName, opts)
      }
      _ <- Tree.raiseFailures(results.flatten, Nil, datasets.size)
    yield ()

  /** 木の中の1つのdatasetをバックアップする。失敗した場合はdataset名とエラーを返す */
  private def backupTreeMember(
      ds: DatasetInfo,
      position: String,
      snapshot: String,
      opts: BackupOptions
  ): IO[Option[Tree.Failure]] =
    // ZFSネイティブ暗号化されたdatasetは、平文で送らないようにrawにする
    val mode = if ds.nativeEncrypted then SendMode.Raw else opts.mode
    val note = if ds.nativeEncrypted && opts.mode != SendMode.Raw then " (encrypted; sending raw)" else ""
    val child = Backup(ctx.forDataset(ds.name))
    val childOpts = opts.forTreeMember(snapshot, mode)
    val run =
      for
        locals <- zfs.listSnapshots(ds.name)
        _ <-
          if locals.exists(_.shortName == snapshot) then child.backupDataset(childOpts, autoFull = true)
          else err(s"warning: ${ds.name} has no snapshot @$snapshot; skipping")
      yield ()

    out(s"== $position ${ds.name}$note") *> run.attempt.flatMap {
      case Right(()) => IO.pure(None)
      case Left(e)   => err(s"error: ${ds.name}: ${e.getMessage}").as(Some((dataset = ds.name, error = e)))
    }

  /** @param autoFull trueなら、S3にバックアップが1つもないdatasetをfullで送る */
  private def backupDataset(opts: BackupOptions, autoFull: Boolean): IO[Unit] =
    for
      locals <- zfs.listSnapshots(common.dataset)
      target <- Planner.selectTarget(locals, common.snapshotPrefix, opts.snapshot).liftTo[IO]
      index <- Reindex(ctx).loadOrCreateIndex
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
        toolVersion = BuildInfo.version,
        partSize = Some(partSize)
      )
      _ <- out(
        s"${parentName.fold("full")(p => s"incremental from $p")} -> ${step.child.name}: " +
          s"estimated $estimated bytes, part size $partSize, key $key"
      )
      _ <- out(s"  ${ZfsCommands.show(ZfsCommands.send(step.child.name, parentName, raw))}")
      _ <-
        if common.dryRun then IO.unit
        else upload(key, step.child, header, parentName, partSize, opts, s"$position ${step.child.name}")
    yield ()

  private def upload(
      key: String,
      snapshot: Snapshot,
      header: Header,
      parentName: Option[String],
      partSize: Long,
      opts: BackupOptions,
      label: String
  ): IO[Unit] =
    val states = opts.stateDir.filter(_ => opts.isResumable).map(StateStore(_, progress.log))
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
          HeaderCodec.decode(r.state.header.toArray).leftMap(Z3SError.Runtime(_)).liftTo[IO].map { h =>
            (r.state.header.toArray, h, r.state.partSize)
          }
        case None => IO.pure((HeaderCodec.encode(header), header, partSize.toInt))
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
                  (Stream.chunk(Chunk.array(ctx.sha256(headerBytes))) ++ compressed).through(Age.encrypt(recipients))
                )
      result <- ctx.uploader.upload(
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
      _ <- savePartDigests(snapshot, PartDigests(effectivePartSize.toLong, result.size, result.partMd5s))
      entry = IndexEntry(key, effectiveHeader, result.size, result.etag, uploadedAt.toString)
      _ <- ctx.indexStore.update(_.upsert(entry)).void.adaptError { case e =>
        Z3SError.Runtime(s"$key was uploaded, but updating the index failed (${e.getMessage}); run `z3s reindex`", e)
      }
    yield ()

  /** 失敗してもバックアップ自体は使えるので、警告にとどめる。一覧がなくても、オブジェクト全体を読めばETagと照合できる */
  private def savePartDigests(snapshot: Snapshot, digests: PartDigests): IO[Unit] =
    val key = common.partDigestsKey(snapshot)
    store
      .put(key, PartDigestsCodec.encode(digests))
      .handleErrorWith(e => err(s"warning: failed to save the part MD5s to $key: ${e.getMessage}"))

  /** 中断したuploadの状態があり、同じスナップショットとmodeなら再開する。そうでなければ古いuploadをabortする */
  private def prepareResume(states: StateStore, key: String, header: Header, noResume: Boolean): IO[Option[Resume]] =
    states.load(common.bucket, key).flatMap {
      case None => IO.pure(None)
      case Some(state) =>
        val mismatch: Option[String] =
          if noResume then Some("--no-resume was given")
          else
            HeaderCodec.decode(state.header.toArray) match
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
