package dev.capslock.z3s

import cats.effect.{IO, Ref}
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import fs2.{Pipe, Stream}
import scala.concurrent.duration.*

enum ProgressMode:
  /** 端末ならBar、そうでなければLog */
  case Auto
  /** 1行を書き換えるバー */
  case Bar
  /** 10%ごとか60秒ごとに1行ずつ出す */
  case Log
  case None

object ProgressMode:
  def parse(s: String): Either[String, ProgressMode] = s match
    case "auto" => Right(Auto)
    case "bar"  => Right(Bar)
    case "log"  => Right(Log)
    case "none" => Right(None)
    case other  => Left(s"unknown progress mode: $other (expected auto, bar, log or none)")

  /** 標準出力が端末かを調べる。Java 22以降はConsole#isTerminalで判定する */
  def resolve(mode: ProgressMode): ProgressMode = mode match
    case Auto =>
      val terminal = Option(System.console()).exists { c =>
        scala.util.Try(c.getClass.getMethod("isTerminal").invoke(c).asInstanceOf[Boolean]).getOrElse(true)
      }
      if terminal then Bar else Log
    case other => other

/** 流れたバイト数を数え、見積もりに対する進捗を標準エラー出力に表示する */
final class Progress(mode: ProgressMode, print: String => IO[Unit] = s => IO(System.err.print(s))):
  private val BarInterval = 500.millis
  private val LogInterval = 60.seconds

  /** 進捗表示と混ざらないように1行出力する。barモードでは表示中のバーの行を消してから書く */
  def log(message: String): IO[Unit] =
    if mode == ProgressMode.Bar then print(s"\r\u001b[K$message\n") else print(s"$message\n")

  /** @param detail バイト数とは別に表示する情報(パートの数など) */
  def track(label: String, total: Option[Long], detail: IO[Option[String]] = IO.pure(None)): Pipe[IO, Byte, Byte] =
    in =>
      mode match
        case ProgressMode.None | ProgressMode.Auto => in
        case _ =>
          Stream.eval((Ref.of[IO, Long](0L), IO.monotonic).tupled).flatMap { (count, start) =>
            val elapsed = IO.monotonic.map(_ - start)
            val counted = in.chunks.evalTap(c => count.update(_ + c.size)).unchunks
            val finish = (count.get, elapsed).flatMapN((n, t) => print(Progress.finishLine(mode, label, n, t)))
            counted.concurrently(reporter(label, total, count, elapsed, detail)).onFinalizeCase {
              case ExitCase.Succeeded => finish
              // 失敗時はバーの行を改行で閉じ、後に出るエラーメッセージと同じ行にしない
              case _ => print("\n").whenA(mode == ProgressMode.Bar)
            }
          }

  private def reporter(
      label: String,
      total: Option[Long],
      count: Ref[IO, Long],
      elapsed: IO[FiniteDuration],
      detail: IO[Option[String]]
  ) =
    mode match
      case ProgressMode.Bar =>
        Stream
          .fixedRate[IO](BarInterval)
          .evalMap(_ =>
            (count.get, elapsed, detail).flatMapN((n, t, d) => print(s"\r${Progress.line(label, n, total, t, d)}\u001b[K"))
          )
      case _ =>
        // 前回出力した10%刻みの段階と時刻を覚え、どちらかが進んだら1行出す
        Stream
          .fixedRate[IO](1.second)
          .evalMapAccumulate((0, Duration.Zero: FiniteDuration)) { case ((step, last), _) =>
            (count.get, elapsed, detail).flatMapN { (n, t, d) =>
              val now = Progress.percent(n, total).map(p => (p / 10).toInt).getOrElse(0)
              if now > step || t - last >= LogInterval then
                print(s"${Progress.line(label, n, total, t, d)}\n").as(((now max step), t), ())
              else IO.pure(((step, last), ()))
            }
          }

object Progress:
  private val BarWidth = 24

  /** 見積もりを超えた場合は完了と区別するため99.9%で止める */
  def percent(done: Long, total: Option[Long]): Option[Double] =
    total.filter(_ > 0).map(t => math.min(done.toDouble / t * 100, 99.9))

  def formatBytes(n: Long): String =
    val units = List("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    var v = n.toDouble
    var i = 0
    while v >= 1024 && i < units.size - 1 do
      v /= 1024
      i += 1
    if i == 0 then s"$n B" else f"$v%.1f ${units(i)}"

  def formatDuration(d: FiniteDuration): String =
    val s = d.toSeconds
    f"${s / 3600}%02d:${s / 60 % 60}%02d:${s % 60}%02d"

  def line(
      label: String,
      done: Long,
      total: Option[Long],
      elapsed: FiniteDuration,
      detail: Option[String] = scala.None
  ): String =
    val rate = if elapsed.toMillis > 0 then done * 1000 / elapsed.toMillis else 0L
    val parts = percent(done, total) match
      case Some(p) =>
        val filled = (p / 100 * BarWidth).toInt
        val bar = "#" * filled + "." * (BarWidth - filled)
        val eta = total.filter(t => done < t && rate > 0).map(t => s"ETA ${formatDuration(((t - done) / rate).seconds)}")
        List(s"[$bar]", f"$p%5.1f%%", s"${formatBytes(done)} / ~${formatBytes(total.get)}") ++ eta
      case scala.None => List(formatBytes(done))
    ((label :: parts) ++ detail :+ s"${formatBytes(rate)}/s").mkString("  ")

  def finishLine(mode: ProgressMode, label: String, done: Long, elapsed: FiniteDuration): String =
    val text = s"$label  done: ${formatBytes(done)} in ${formatDuration(elapsed)}"
    mode match
      case ProgressMode.Bar => s"\r$text\u001b[K\n"
      case _                => s"$text\n"
