package dev.capslock.z3s

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import fs2.{Chunk, Stream}
import scala.concurrent.duration.*

class ProgressSuite extends IOSpec:
  test("format") {
    Progress.formatBytes(512) should === ("512 B")
    Progress.formatBytes(1536) should === ("1.5 KiB")
    Progress.formatBytes(5L * 1024 * 1024 * 1024) should === ("5.0 GiB")
    Progress.formatDuration(3725.seconds) should === ("01:02:05")
  }

  test("percent stays below 100 when the estimate is exceeded") {
    Progress.percent(50, Some(200)) should === (Some(25.0))
    Progress.percent(300, Some(200)) should === (Some(99.9))
    Progress.percent(10, Some(0)) should === (None)
    Progress.percent(10, None) should === (None)
  }

  test("line") {
    val mib = 1024L * 1024
    Progress.line("[1/2] tank/data@a", 25 * mib, Some(100 * mib), 5.seconds) should === (
      "[1/2] tank/data@a  [######..................]   25.0%  25.0 MiB / ~100.0 MiB  ETA 00:00:15  5.0 MiB/s"
    )
    // 見積もりを超えたらETAを出さない
    Progress.line("x", 150 * mib, Some(100 * mib), 5.seconds) should not include "ETA"
    Progress.line("x", 2048, None, 2.seconds) should === ("x  2.0 KiB  1.0 KiB/s")
  }

  /** 10バイトずつ1.5秒おきに流し、表示された文字列と流れたデータを返す */
  def run(mode: ProgressMode, fail: Boolean = false): IO[(List[String], Either[Throwable, List[Byte]])] =
    TestControl.executeEmbed {
      for
        printed <- Ref.of[IO, List[String]](Nil)
        progress = Progress(mode, s => printed.update(_ :+ s))
        source = Stream
          .range(0, 10)
          .covary[IO]
          .evalMap(i => IO.sleep(1500.millis).as(Chunk.array(Array.fill[Byte](10)(i.toByte))))
          .unchunks ++ (if fail then Stream.raiseError[IO](new Exception("boom")) else Stream.empty)
        result <- source.through(progress.track("label", Some(100))).compile.toList.attempt
        out <- printed.get
      yield (out, result)
    }

  test("log mode prints a line at each 10% step and a final line") {
    run(ProgressMode.Log).map { (out, result) =>
      result.map(_.size) should === (Right(100))
      val percents = out.init.map(_.split("  ")(2).trim)
      percents.distinct should === (percents)
      percents.size should be >= 5
      forAll(out)(_ should endWith("\n"))
      out.last should === ("label  done: 100 B in 00:00:15\n")
    }
  }

  test("bar mode rewrites one line and ends it with a newline") {
    run(ProgressMode.Bar).map { (out, result) =>
      result.map(_.size) should === (Right(100))
      forAll(out.init) { s =>
        s should startWith("\r")
        s should not include "\n"
      }
      out.size should be > 10
      out.last should === ("\rlabel  done: 100 B in 00:00:15\u001b[K\n")
    }
  }

  test("bar mode ends the line when the stream fails") {
    run(ProgressMode.Bar, fail = true).map { (out, result) =>
      result.left.value.getMessage should === ("boom")
      out.last should === ("\n")
    }
  }

  test("none mode prints nothing") {
    run(ProgressMode.None).map { (out, result) =>
      out should === (Nil)
      result.map(_.size) should === (Right(100))
    }
  }

  test("mode parsing") {
    ProgressMode.parse("log") should === (Right(ProgressMode.Log))
    ProgressMode.parse("fancy").left.value should include("fancy")
    ProgressMode.resolve(ProgressMode.Bar) should === (ProgressMode.Bar)
  }
