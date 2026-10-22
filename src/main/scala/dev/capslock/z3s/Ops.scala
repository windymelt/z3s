package dev.capslock.z3s

import cats.data.NonEmptyList
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import fs2.{Chunk, Pipe, Pull, Stream}
import io.bullet.borer.{Json, Writer}
import java.security.MessageDigest
import java.time.Instant
import java.time.format.DateTimeFormatter
import scodec.bits.ByteVector
import dev.capslock.z3s.age.{Age, X25519Identity, X25519Recipient}

final case class Common(
    bucket: String,
    prefix: String,
    dataset: String,
    snapshotPrefix: Option[String],
    dryRun: Boolean,
    concurrency: Int
):
  def objectKey(snapshotName: String): String = s"$prefix$snapshotName"
  def objectPrefix: String = s"$prefix$dataset@"
  def indexKey: String = s"$prefix$dataset/.z3s-index.cbor"

final case class BackupOptions(
    full: Boolean,
    snapshot: Option[String],
    mode: SendMode,
    storageClass: Option[String]
)

final case class RestoreOptions(
    snapshot: String,
    target: Option[String],
    force: Boolean,
    identities: Option[NonEmptyList[X25519Identity]]
)

final class Ops(common: Common, zfs: Zfs, store: ObjectStore):
  private val indexStore = IndexStore(store, common.indexKey)
  private def out(s: String): IO[Unit] = IO.println(s)
  private def err(s: String): IO[Unit] = IO.consoleForIO.errorln(s)
  private def now: IO[Instant] = Clock[IO].realTimeInstant

  private def sha256(bytes: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(bytes)

  // ---- reindex ----

  def reindex: IO[Index] =
    for
      entries <- store
        .list(common.objectPrefix)
        .parEvalMap(common.concurrency)(readEntry)
        .unNone
        .compile
        .toList
      index = Index(Index.SchemaVersion, entries.sortBy(_.header.createtxg))
      _ <-
        if common.dryRun then out(s"[dry-run] would write index with ${entries.size} entries to ${common.indexKey}")
        else indexStore.update(_ => index).void
    yield index

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
    for
      locals <- zfs.listSnapshots(common.dataset)
      target <- Planner.selectTarget(locals, common.snapshotPrefix, opts.snapshot).liftTo[IO]
      index <- loadIndex
      plan <- Planner
        .planBackup(locals, common.snapshotPrefix, target, index, opts.full, opts.mode.kind)
        .liftTo[IO]
      _ <- plan match
        case BackupPlan.UpToDate(s) => out(s"${s.name} is already backed up")
        case BackupPlan.Steps(steps) =>
          out(s"plan: ${steps.size} object(s) in ${opts.mode.kind.label} mode") *>
            steps.traverse_(sendOne(_, opts))
    yield ()

  private def sendOne(step: SendStep, opts: BackupOptions): IO[Unit] =
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
      _ <- if common.dryRun then IO.unit else upload(key, header, parentName, partSize, opts)
    yield ()

  private def upload(
      key: String,
      header: Header,
      parentName: Option[String],
      partSize: Long,
      opts: BackupOptions
  ): IO[Unit] =
    val headerBytes = Cbor.encodeHeader(header)
    val frame = Frame.encode(Frame(Frame.CurrentVersion, ByteVector(headerBytes)))
    val metadata = Map("z3s-format" -> Frame.CurrentVersion.toString) ++ parentName.map("z3s-parent" -> _)
    val raw = opts.mode == SendMode.Raw
    val send = zfs.send(header.snapshot, parentName, raw)
    for
      _ <- IO
        .raiseError(Z3SError.Runtime(s"header is too large: ${headerBytes.length} bytes"))
        .whenA(headerBytes.length > Frame.MaxHeaderLength)
      _ <- store.head(key).flatMap {
        case Some(_) => IO.raiseError(Z3SError.Runtime(s"object already exists: $key"))
        case None    => IO.unit
      }
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
      result <- store.uploadMultipart(
        key,
        metadata,
        opts.storageClass,
        partSize.toInt,
        common.concurrency,
        Stream.chunk(Chunk.byteVector(frame)) ++ payload
      )
      uploadedAt <- now
      _ <- out(s"uploaded $key (${result.size} bytes)")
      entry = IndexEntry(key, header, result.size, result.etag, uploadedAt.toString)
      _ <- indexStore.update(_.upsert(entry)).void.adaptError { case e =>
        Z3SError.Runtime(s"$key was uploaded, but updating the index failed (${e.getMessage}); run `z3s reindex`", e)
      }
    yield ()

  // ---- restore ----

  def restore(opts: RestoreOptions): IO[Unit] =
    val target = opts.target.getOrElse(common.dataset)
    val snapshotName = s"${common.dataset}@${opts.snapshot}"
    for
      index <- loadIndex
      chain <- Planner.restoreChain(index, snapshotName).liftTo[IO]
      _ <- IO
        .raiseError(Z3SError.Usage("--identity is required to restore age-encrypted objects"))
        .whenA(opts.identities.isEmpty && chain.exists(e => isAge(e.header.mode)))
      exists <- zfs.datasetExists(target)
      targetLocals <- if exists then zfs.listSnapshots(target) else IO.pure(Nil)
      remaining = Planner.remainingToReceive(chain, targetLocals)
      _ <-
        if remaining.isEmpty then out(s"$target already has $snapshotName")
        else
          out(s"plan: receive ${remaining.size} object(s) into $target") *>
            remaining.traverse_(receiveOne(_, target, opts))
    yield ()

  private def isAge(mode: SendMode): Boolean = mode match
    case SendMode.Plain(_, Encryption.Age(_)) => true
    case _                                    => false

  private def receiveOne(entry: IndexEntry, target: String, opts: RestoreOptions): IO[Unit] =
    val describe = s"${entry.key} | ${ZfsCommands.show(ZfsCommands.recv(target, opts.force))}"
    if common.dryRun then out(s"[dry-run] $describe")
    else
      out(describe) *>
        zfs.recv(target, opts.force, Frame.split(store.get(entry.key))(decodePayload(entry, opts)))

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

  def status(refresh: Boolean, json: Boolean): IO[Unit] =
    for
      index <- if refresh then reindex else loadIndex
      exists <- zfs.datasetExists(common.dataset)
      locals <- if exists then zfs.listSnapshots(common.dataset) else IO.pure(Nil)
      health = Planner.health(index)
      localGuids = locals.map(_.guid).toSet
      rows = index.entries.sortBy(_.header.createtxg).map { e =>
        StatusRow(e, health(e.key), localGuids.contains(e.header.guid))
      }
      _ <- if json then out(StatusRow.json(rows)) else out(StatusRow.table(rows))
    yield ()

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

  def json(rows: List[StatusRow]): String =
    def row(w: Writer, r: StatusRow): Writer =
      w.writeMapHeader(11)
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

    given io.bullet.borer.Encoder[List[StatusRow]] = io.bullet.borer.Encoder { (w, rs) =>
      w.writeArrayHeader(rs.size)
      rs.foreach(row(w, _))
      w
    }
    Json.encode(rows).toUtf8String
