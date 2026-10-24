package dev.capslock.z3s

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.monovore.decline.*
import java.net.URI
import java.nio.file.{Files, Path}
import dev.capslock.z3s.age.{X25519Identity, X25519Recipient}

final case class Connection(region: Option[String], endpoint: Option[URI])

enum Cmd:
  case Backup(opts: BackupOptions)
  case Restore(snapshot: String, target: Option[String], force: Boolean, identity: Option[Path], recursive: Boolean)
  case Status(refresh: Boolean, json: Boolean, recursive: Boolean)
  case Reindex

object Cli:
  private def withEnv[A: Argument](long: String, env: String, help: String): Opts[A] =
    Opts.option[A](long, help).orElse(Opts.env[A](env, help))

  val common: Opts[Common] =
    (
      withEnv[String]("bucket", "Z3S_BUCKET", "Bucket to store backups in."),
      withEnv[String]("prefix", "Z3S_PREFIX", "Prefix of object keys.").withDefault(""),
      withEnv[String]("dataset", "Z3S_DATASET", "Dataset to back up (e.g. tank/data)."),
      withEnv[String]("snapshot-prefix", "Z3S_SNAPSHOT_PREFIX", "Only consider snapshots whose name starts with this.").orNone,
      Opts.flag("dry-run", "Print the plan without writing to S3 or running zfs recv.").orFalse,
      Opts
        .option[Int]("concurrency", "Number of parallel S3 requests (default 4).")
        .withDefault(4)
        .validate("--concurrency must be positive")(_ > 0),
      withEnv[String]("progress", "Z3S_PROGRESS", "auto, bar, log or none (default auto).")
        .withDefault("auto")
        .mapValidated(ProgressMode.parse(_).toValidatedNel)
    ).mapN(Common.apply)

  val connection: Opts[Connection] =
    (
      withEnv[String]("region", "AWS_REGION", "AWS region.").orNone,
      withEnv[URI]("endpoint", "Z3S_ENDPOINT", "Endpoint of S3-compatible storage.").orNone
    ).mapN(Connection.apply)

  private def sendMode(
      raw: Boolean,
      compress: Option[String],
      encrypt: Option[String],
      recipients: List[String]
  ): ValidatedNel[String, SendMode] =
    if raw then
      Validated.condNel(
        compress.isEmpty && encrypt.isEmpty && recipients.isEmpty,
        SendMode.Raw,
        "--raw cannot be combined with --compress, --encrypt or --recipient"
      )
    else
      val compression = compress.fold(Compression.Default.validNel)(Compression.parse(_).toValidatedNel)
      val encryption: ValidatedNel[String, Encryption] = (encrypt.getOrElse("none"), recipients) match
        case ("none", Nil) => Encryption.None.validNel
        case ("none", _)   => "--recipient requires --encrypt age".invalidNel
        case ("age", Nil)  => "--encrypt age requires at least one --recipient".invalidNel
        case ("age", r :: rs) =>
          (r :: rs).traverse(X25519Recipient.parse(_).toValidatedNel).as(Encryption.Age(NonEmptyList(r, rs)))
        case (other, _) => s"unknown encryption: $other".invalidNel
      (compression, encryption).mapN(SendMode.Plain(_, _))

  val backup: Opts[Cmd] =
    (
      Opts.flag("full", "Send a full stream instead of an incremental one.").orFalse,
      Opts.option[String]("snapshot", "Snapshot to back up (the part after @). Defaults to the latest one.").orNone,
      Opts.flag("raw", "Use zfs send -w (no compression or encryption).").orFalse,
      Opts.option[String]("compress", "none, gzip[:1-9], zstd[:1-19], xz[:0-9] or bzip2[:1-9]. Default zstd:3.").orNone,
      Opts.option[String]("encrypt", "none or age.").orNone,
      Opts.options[String]("recipient", "age recipient (age1...). Can be repeated.").orEmpty,
      Opts.option[String]("storage-class", "S3 storage class.").orNone,
      Opts.flag("no-resume", "Discard an interrupted upload and start over.").orFalse,
      withEnv[Path]("state-dir", "Z3S_STATE_DIR", s"Where to keep resume state (default $defaultStateDir).")
        .withDefault(defaultStateDir),
      recursiveFlag
    ).tupled.mapValidated {
      case (full, snapshot, raw, compress, encrypt, recipients, storageClass, noResume, stateDir, recursive) =>
        sendMode(raw, compress, encrypt, recipients).map(mode =>
          Cmd.Backup(BackupOptions(full, snapshot, mode, storageClass, noResume, Some(stateDir), recursive))
        )
    }

  private def recursiveFlag: Opts[Boolean] =
    Opts.flag("recursive", "Also process the descendant datasets.", short = "r").orFalse

  /** XDG Base Directory仕様の状態ディレクトリ */
  private def defaultStateDir: Path =
    sys.env
      .get("XDG_STATE_HOME")
      .filter(_.nonEmpty)
      .map(Path.of(_))
      .getOrElse(Path.of(sys.props("user.home"), ".local", "state"))
      .resolve("z3s")

  val restore: Opts[Cmd] =
    (
      Opts.argument[String]("SNAPSHOT"),
      Opts.option[String]("target", "Dataset to receive into. Defaults to --dataset.").orNone,
      Opts.flag("force", "Use zfs recv -F.", short = "F").orFalse,
      Opts.option[Path]("identity", "age identity file (AGE-SECRET-KEY-1...).").orNone,
      recursiveFlag
    ).mapN(Cmd.Restore.apply)

  val status: Opts[Cmd] =
    (
      Opts.flag("refresh", "Run reindex first.").orFalse,
      Opts.flag("json", "Print JSON.").orFalse,
      recursiveFlag
    ).mapN(Cmd.Status.apply)

  private def sub(name: String, help: String, opts: Opts[Cmd]): Opts[(Common, Connection, Cmd)] =
    Opts.subcommand(name, help)((common, connection, opts).tupled)

  val command: Command[(Common, Connection, Cmd)] =
    Command("z3s", s"Back up ZFS snapshots to S3 (version ${BuildInfo.version}).")(
      sub("backup", "Send snapshots to S3.", backup)
        .orElse(sub("restore", "Receive a snapshot from S3.", restore))
        .orElse(sub("status", "Show backups recorded in the index.", status))
        .orElse(sub("reindex", "Rebuild the index from object headers.", Opts(Cmd.Reindex)))
    )

object Main extends IOApp:
  private def readIdentities(path: Path): IO[NonEmptyList[X25519Identity]] =
    IO.blocking(Files.readString(path))
      .adaptError { case e => Z3SError.Usage(s"cannot read identity file $path: ${e.getMessage}") }
      .flatMap(s => X25519Identity.parseFile(s).leftMap(e => Z3SError.Usage(s"$path: $e")).liftTo[IO])

  def execute(common: Common, connection: Connection, cmd: Cmd): IO[Unit] =
    val identities = cmd match
      case Cmd.Restore(_, _, _, identity, _) => identity.traverse(readIdentities)
      case _                                 => IO.pure(None)
    identities.flatMap { ids =>
      S3ObjectStore.client(connection.region, connection.endpoint).use { client =>
        val ops = Ops(common, ProcessZfs(), S3ObjectStore(client, common.bucket))
        cmd match
          case Cmd.Backup(opts) => ops.backup(opts)
          case Cmd.Restore(snapshot, target, force, _, recursive) =>
            ops.restore(RestoreOptions(snapshot, target, force, ids, recursive))
          case Cmd.Status(refresh, json, recursive) => ops.status(refresh, json, recursive)
          case Cmd.Reindex => ops.reindex.flatMap(i => IO.println(s"indexed ${i.entries.size} object(s)"))
      }
    }

  def run(args: List[String]): IO[ExitCode] =
    Cli.command.parse(args, sys.env) match
      case Left(help) if help.errors.isEmpty => IO.println(help).as(ExitCode.Success)
      case Left(help)                        => IO.consoleForIO.errorln(help).as(ExitCode(2))
      case Right((common, connection, cmd)) =>
        execute(common, connection, cmd).as(ExitCode.Success).handleErrorWith {
          case e: Z3SError => IO.consoleForIO.errorln(s"error: ${e.getMessage}").as(ExitCode(e.exitCode))
          case e           => IO.consoleForIO.errorln(s"error: $e").as(ExitCode.Error)
        }
