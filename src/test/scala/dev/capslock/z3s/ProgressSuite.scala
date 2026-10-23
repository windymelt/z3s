package dev.capslock.z3s

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class ProgressSuite extends CatsEffectSuite:
  test("format") {
    assertEquals(Progress.formatBytes(512), "512 B")
    assertEquals(Progress.formatBytes(1536), "1.5 KiB")
    assertEquals(Progress.formatBytes(5L * 1024 * 1024 * 1024), "5.0 GiB")
    assertEquals(Progress.formatDuration(3725.seconds), "01:02:05")
  }

  test("percent stays below 100 when the estimate is exceeded") {
    assertEquals(Progress.percent(50, Some(200)), Some(25.0))
    assertEquals(Progress.percent(300, Some(200)), Some(99.9))
    assertEquals(Progress.percent(10, Some(0)), None)
    assertEquals(Progress.percent(10, None), None)
  }

  test("line") {
    val mib = 1024L * 1024
    assertEquals(
      Progress.line("[1/2] tank/data@a", 25 * mib, Some(100 * mib), 5.seconds),
      "[1/2] tank/data@a  [######..................]   25.0%  25.0 MiB / ~100.0 MiB  ETA 00:00:15  5.0 MiB/s"
    )
    // 見積もりを超えたらETAを出さない
    assert(!Progress.line("x", 150 * mib, Some(100 * mib), 5.seconds).contains("ETA"))
    assertEquals(Progress.line("x", 2048, None, 2.seconds), "x  2.0 KiB  1.0 KiB/s")
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
      assertEquals(result.map(_.size), Right(100))
      val percents = out.init.map(_.split("  ")(2).trim)
      assertEquals(percents.distinct, percents)
      assert(percents.size >= 5, out.mkString)
      assert(out.forall(_.endsWith("\n")))
      assertEquals(out.last, "label  done: 100 B in 00:00:15\n")
    }
  }

  test("bar mode rewrites one line and ends it with a newline") {
    run(ProgressMode.Bar).map { (out, result) =>
      assertEquals(result.map(_.size), Right(100))
      assert(out.init.forall(s => s.startsWith("\r") && !s.contains("\n")), out.mkString)
      assert(out.size > 10)
      assertEquals(out.last, "\rlabel  done: 100 B in 00:00:15\u001b[K\n")
    }
  }

  test("bar mode ends the line when the stream fails") {
    run(ProgressMode.Bar, fail = true).map { (out, result) =>
      assert(result.isLeft)
      assertEquals(out.last, "\n")
    }
  }

  test("none mode prints nothing") {
    run(ProgressMode.None).map { (out, result) =>
      assertEquals(out, Nil)
      assertEquals(result.map(_.size), Right(100))
    }
  }

  test("mode parsing") {
    assertEquals(ProgressMode.parse("log"), Right(ProgressMode.Log))
    assert(ProgressMode.parse("fancy").isLeft)
    assertEquals(ProgressMode.resolve(ProgressMode.Bar), ProgressMode.Bar)
  }
