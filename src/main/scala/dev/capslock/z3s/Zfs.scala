package dev.capslock.z3s

import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import fs2.io.process.{ProcessBuilder, Redirect}

/** encryptedはZFSネイティブ暗号化が有効か */
final case class DatasetInfo(name: String, encrypted: Boolean)

trait Zfs:
  /** rootとその子孫のfilesystemとvolumeを、名前の順(親が子より先)で返す */
  def listDatasets(root: String): IO[List[DatasetInfo]]
  /** createtxgの昇順で返す */
  def listSnapshots(dataset: String): IO[List[Snapshot]]
  def datasetExists(dataset: String): IO[Boolean]
  def estimateSize(snapshot: String, parent: Option[String], raw: Boolean): IO[Long]
  /** 終了コードが0以外ならストリームを失敗させる */
  def send(snapshot: String, parent: Option[String], raw: Boolean): Stream[IO, Byte]
  def recv(target: String, force: Boolean, input: Stream[IO, Byte]): IO[Unit]

object ZfsCommands:
  def list(dataset: String): List[String] =
    List("list", "-H", "-p", "-t", "snapshot", "-d", "1", "-o", "name,guid,createtxg", "-s", "createtxg", dataset)

  def sendArgs(snapshot: String, parent: Option[String], raw: Boolean): List[String] =
    (if raw then List("-w") else Nil) ++ parent.toList.flatMap(p => List("-i", p)) :+ snapshot

  def send(snapshot: String, parent: Option[String], raw: Boolean): List[String] =
    "send" :: sendArgs(snapshot, parent, raw)

  def estimate(snapshot: String, parent: Option[String], raw: Boolean): List[String] =
    "send" :: "-nvP" :: sendArgs(snapshot, parent, raw)

  def recv(target: String, force: Boolean): List[String] =
    ("recv" :: (if force then List("-F") else Nil)) :+ target

  def show(args: List[String]): String = ("zfs" :: args).mkString(" ")

  def listDatasets(root: String): List[String] =
    List("list", "-H", "-o", "name,encryption", "-t", "filesystem,volume", "-r", root)

  /** encryptionは暗号化していなければ"off"になる。値のない"-"も暗号化なしとして扱う */
  def parseDatasets(out: String): Either[String, List[DatasetInfo]] =
    out.linesIterator.filter(_.nonEmpty).toList.traverse { line =>
      line.split("\t") match
        case Array(name, encryption) => Right(DatasetInfo(name, encryption != "off" && encryption != "-"))
        case _                       => Left(s"cannot parse zfs list output: $line")
    }.map(_.sortBy(_.name))

  /** zfs recvが失敗したときに考えられる原因 */
  def recvHints(force: Boolean): List[String] =
    val open = "is the target dataset open? zfs recv fails if a shell or process is using files in it."
    // -Fは受信前に最新のスナップショットへロールバックするため、変更は失敗の原因にならない
    val modified =
      "has the target dataset been modified since its latest snapshot? " +
        "--force rolls it back to that snapshot before receiving, which discards the changes."
    if force then List(open) else List(open, modified)

  def parseList(out: String): Either[String, List[Snapshot]] =
    out.linesIterator.filter(_.nonEmpty).toList.traverse { line =>
      line.split("\t") match
        case Array(name, guid, txg) =>
          Either
            .catchNonFatal(Snapshot(name, java.lang.Long.parseUnsignedLong(guid), txg.toLong))
            .leftMap(_ => s"cannot parse zfs list output: $line")
        case _ => Left(s"cannot parse zfs list output: $line")
    }

  /** `zfs send -nvP`の出力から`size`行を読む */
  def parseEstimate(out: String): Either[String, Long] =
    out.linesIterator
      .map(_.split("\t"))
      .collect { case Array("size", n) => n.trim.toLongOption }
      .flatten
      .toList
      .lastOption
      .toRight(s"cannot find size in zfs send -nvP output: ${out.take(500)}")

final class ProcessZfs(command: String = "zfs") extends Zfs:
  private def run(args: List[String], mergeStderr: Boolean = false): IO[(Int, String, String)] =
    ProcessBuilder(command, args)
      .withRedirectErrorStream(mergeStderr)
      .spawn[IO]
      .use { p =>
        val out = p.stdout.through(fs2.text.utf8.decode).compile.string
        val err =
          if mergeStderr then IO.pure("") else p.stderr.through(fs2.text.utf8.decode).compile.string
        (out, err, p.exitValue).parMapN((o, e, code) => (code, o, e))
      }

  private def failed(args: List[String], code: Int, err: String): Z3SError =
    Z3SError.Runtime(s"`${ZfsCommands.show(args)}` exited with $code: ${err.trim}")

  def listSnapshots(dataset: String): IO[List[Snapshot]] =
    val args = ZfsCommands.list(dataset)
    run(args).flatMap {
      case (0, out, _) => ZfsCommands.parseList(out).leftMap(Z3SError.Runtime(_)).liftTo[IO]
      case (code, _, err) => IO.raiseError(failed(args, code, err))
    }

  def datasetExists(dataset: String): IO[Boolean] =
    run(List("list", "-H", "-o", "name", dataset)).map(_._1 == 0)

  def listDatasets(root: String): IO[List[DatasetInfo]] =
    val args = ZfsCommands.listDatasets(root)
    run(args).flatMap {
      case (0, out, _)    => ZfsCommands.parseDatasets(out).leftMap(Z3SError.Runtime(_)).liftTo[IO]
      case (code, _, err) => IO.raiseError(failed(args, code, err))
    }

  def estimateSize(snapshot: String, parent: Option[String], raw: Boolean): IO[Long] =
    // 出力先がstdoutとstderrのどちらになるかはOpenZFSのバージョンで異なるため、両方を読む
    val args = ZfsCommands.estimate(snapshot, parent, raw)
    run(args, mergeStderr = true).flatMap {
      case (0, out, _) => ZfsCommands.parseEstimate(out).leftMap(Z3SError.Runtime(_)).liftTo[IO]
      case (code, out, _) => IO.raiseError(failed(args, code, out))
    }

  def send(snapshot: String, parent: Option[String], raw: Boolean): Stream[IO, Byte] =
    val args = ZfsCommands.send(snapshot, parent, raw)
    Stream
      .resource(ProcessBuilder(command, args).withStderr(Redirect.Inherit).spawn[IO])
      .flatMap { p =>
        p.stdout ++ Stream.exec(p.exitValue.flatMap { code =>
          IO.raiseError(failed(args, code, "")).whenA(code != 0)
        })
      }

  def recv(target: String, force: Boolean, input: Stream[IO, Byte]): IO[Unit] =
    val args = ZfsCommands.recv(target, force)
    def recvFailed(code: Int): Z3SError =
      Z3SError.Runtime(
        (s"`${ZfsCommands.show(args)}` exited with $code" :: ZfsCommands.recvHints(force).map("hint: " + _))
          .mkString("\n")
      )
    ProcessBuilder(command, args)
      .withStdout(Redirect.Inherit)
      .withStderr(Redirect.Inherit)
      .spawn[IO]
      .use { p =>
        input.through(p.stdin).compile.drain.attempt.flatMap {
          case Right(()) =>
            p.exitValue.flatMap(code => IO.raiseError(recvFailed(code)).whenA(code != 0))
          case Left(e) =>
            // zfs recvが先に異常終了した場合は書き込みエラーより終了コードを報告する
            p.isAlive.flatMap {
              case false =>
                p.exitValue.flatMap(code => IO.raiseError(if code != 0 then recvFailed(code) else e))
              case true => IO.raiseError(e)
            }
        }
      }
