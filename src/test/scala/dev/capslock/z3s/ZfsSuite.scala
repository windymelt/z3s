package dev.capslock.z3s

import cats.effect.{IO, Resource}
import fs2.{Chunk, Stream}
import java.nio.file.{Files, Path}

/** zfsコマンドの代わりにシェルスクリプトを実行してProcessZfsを確かめる */
class ZfsSuite extends IOSpec:
  private def script(body: String): Resource[IO, Path] =
    Resource.make(IO.blocking {
      val p = Files.createTempFile("fake-zfs", ".sh")
      Files.writeString(p, s"#!/bin/sh\n$body\n")
      p.toFile.setExecutable(true)
      p
    })(p => IO.blocking(Files.deleteIfExists(p)).void)

  // パイプのバッファより大きくし、読まずに終了するスクリプトへの書き込みを失敗させる
  private val input = Stream.chunk(Chunk.array(new Array[Byte](4 * 1024 * 1024))).covary[IO]

  private def recvError(body: String, force: Boolean): IO[String] =
    script(body).use { p =>
      ProcessZfs(p.toString).recv("pool/restored", force, input).attempt.map(_.left.value.getMessage)
    }

  for (name, body) <- List("after reading the stream" -> "cat >/dev/null; exit 1", "without reading" -> "exit 1") do
    test(s"zfs recv failure shows hints ($name)") {
      recvError(body, force = false).map { msg =>
        msg should startWith("`zfs recv pool/restored` exited with 1")
        msg should include("hint: is the target dataset open?")
        msg should include("hint: has the target dataset been modified since its latest snapshot?")
      }
    }

  test("with --force the modification hint is omitted") {
    recvError("cat >/dev/null; exit 1", force = true).map { msg =>
      msg should startWith("`zfs recv -F pool/restored` exited with 1")
      msg should include("hint: is the target dataset open?")
      msg should not include "modified"
    }
  }

  test("successful zfs recv raises nothing") {
    script("cat >/dev/null; exit 0")
      .use(p => ProcessZfs(p.toString).recv("pool/restored", false, input))
      .attempt
      .map(_ should === (Right(())))
  }

  test("zfs list output of datasets is parsed with their encryption") {
    ZfsCommands.parseDatasets("pool/foo/bar\taes-256-gcm\npool/foo\toff\npool/legacy\t-\n") should === (
      Right(
        List(
          DatasetInfo("pool/foo", nativeEncrypted = false),
          DatasetInfo("pool/foo/bar", nativeEncrypted = true),
          DatasetInfo("pool/legacy", nativeEncrypted = false)
        )
      )
    )
  }
