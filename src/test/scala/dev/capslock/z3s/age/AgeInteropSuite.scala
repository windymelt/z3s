package dev.capslock.z3s.age

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import dev.capslock.z3s.IOSpec

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import scala.jdk.CollectionConverters.*

import AgeTestSupport.*

/** 公式age（AGE_BIN / AGE_KEYGEN_BIN、なければPATH上のage / age-keygen）との相互運用 */
class AgeInteropSuite extends IOSpec:
  private def findBinary(envName: String, name: String): Option[Path] =
    sys.env
      .get(envName)
      .map(Paths.get(_))
      .orElse(
        sys.env
          .getOrElse("PATH", "")
          .split(java.io.File.pathSeparator)
          .iterator
          .map(Paths.get(_, name))
          .find(Files.isExecutable)
      )
      .filter(Files.isExecutable)

  private lazy val ageBin = findBinary("AGE_BIN", "age")
  private lazy val keygenBin = findBinary("AGE_KEYGEN_BIN", "age-keygen")

  private def binaries: (Path, Path) =
    assume(ageBin.isDefined && keygenBin.isDefined, "age / age-keygen not found")
    (ageBin.get, keygenBin.get)

  private val tempDir: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("z3s-age-test")))(dir =>
      IO.blocking(Files.walk(dir).sorted(Comparator.reverseOrder()).iterator.asScala.foreach(Files.delete))
    )

  /** プロセスを実行し、終了コード0なら(stdout, stderr)を返す */
  private def run(cmd: String*): IO[(String, String)] =
    IO.blocking {
      val dir = Files.createTempDirectory("z3s-age-proc")
      val out = dir.resolve("out")
      val err = dir.resolve("err")
      try
        val p = ProcessBuilder(cmd*).redirectOutput(out.toFile).redirectError(err.toFile).start()
        val code = p.waitFor()
        val (o, e) = (Files.readString(out), Files.readString(err))
        if code != 0 then throw RuntimeException(s"${cmd.mkString(" ")} exited with $code: $e")
        (o, e)
      finally
        Files.deleteIfExists(out)
        Files.deleteIfExists(err)
        Files.delete(dir)
    }

  private def keygen(keygenBin: Path, dir: Path): IO[(Path, String)] =
    val keyFile = dir.resolve("key.txt")
    run(keygenBin.toString, "-o", keyFile.toString).flatMap { (_, stderr) =>
      // age-keygenは"Public key: age1..."をstderrに出力する
      val pub = stderr.linesIterator.collectFirst { case s"Public key: $k" => k.trim }
      IO.fromOption(pub)(RuntimeException(s"public key not found in: $stderr")).tupleLeft(keyFile)
    }

  private val sizes = List(0, 100, 65536, 200000)

  test("age-keygen key file is parsed and the public key matches") {
    val (_, kg) = binaries
    tempDir.use { dir =>
      for
        (keyFile, pub) <- keygen(kg, dir)
        contents <- IO.blocking(Files.readString(keyFile))
        (yOut, _) <- run(kg.toString, "-y", keyFile.toString)
      yield
        val ids = X25519Identity.parseFile(contents).value
        ids.toList should have size 1
        ids.head.recipient.encode should === (pub)
        ids.head.recipient.encode should === (yOut.trim)
        X25519Recipient.parse(pub).value.encode should === (pub)
        // 秘密鍵の再エンコードがファイルの行と一致すること
        contents.linesIterator.toList should contain (ids.head.encode)
    }
  }

  test("z3s encrypts, age decrypts") {
    val (age, kg) = binaries
    tempDir.use { dir =>
      for
        (keyFile, pub) <- keygen(kg, dir)
        recipient <- IO.fromEither(X25519Recipient.parse(pub).leftMap(RuntimeException(_)))
        _ <- sizes.traverse_ { size =>
          val plain = randomBytes(size, size.toLong)
          val enc = dir.resolve(s"z3s-$size.age")
          val dec = dir.resolve(s"z3s-$size.out")
          for
            cipher <- encryptBytes(NonEmptyList.one(recipient), plain)
            _ <- IO.blocking(Files.write(enc, cipher))
            _ <- run(age.toString, "-d", "-i", keyFile.toString, "-o", dec.toString, enc.toString)
            decrypted <- IO.blocking(Files.readAllBytes(dec))
          yield withClue(s"size $size: ")(decrypted should === (plain))
        }
      yield succeed
    }
  }

  test("age encrypts, z3s decrypts") {
    val (age, kg) = binaries
    tempDir.use { dir =>
      for
        (keyFile, pub) <- keygen(kg, dir)
        contents <- IO.blocking(Files.readString(keyFile))
        ids <- IO.fromEither(X25519Identity.parseFile(contents).leftMap(RuntimeException(_)))
        _ <- sizes.traverse_ { size =>
          val plain = randomBytes(size, size.toLong + 1)
          val in = dir.resolve(s"age-$size.in")
          val enc = dir.resolve(s"age-$size.age")
          for
            _ <- IO.blocking(Files.write(in, plain))
            _ <- run(age.toString, "-r", pub, "-o", enc.toString, in.toString)
            cipher <- IO.blocking(Files.readAllBytes(enc))
            decrypted <- decryptBytes(ids, cipher)
          yield withClue(s"size $size: ")(decrypted should === (plain))
        }
      yield succeed
    }
  }

  test("age encrypts to multiple recipients, z3s decrypts with the second identity") {
    val (age, _) = binaries
    tempDir.use { dir =>
      for
        a <- X25519Identity.generate
        b <- X25519Identity.generate
        plain = randomBytes(150000)
        in = dir.resolve("multi.in")
        enc = dir.resolve("multi.age")
        _ <- IO.blocking(Files.write(in, plain))
        _ <- run(age.toString, "-r", a.recipient.encode, "-r", b.recipient.encode, "-o", enc.toString, in.toString)
        cipher <- IO.blocking(Files.readAllBytes(enc))
        decrypted <- decryptBytes(NonEmptyList.one(b), cipher)
        // 本実装で生成した鍵をageの鍵ファイルとして使えること
        keyFile = dir.resolve("b.txt")
        _ <- IO.blocking(Files.writeString(keyFile, b.encode + "\n", StandardCharsets.US_ASCII))
        z3sCipher <- encryptBytes(NonEmptyList.of(a.recipient, b.recipient), plain)
        z3sEnc = dir.resolve("z3s-multi.age")
        z3sDec = dir.resolve("z3s-multi.out")
        _ <- IO.blocking(Files.write(z3sEnc, z3sCipher))
        _ <- run(age.toString, "-d", "-i", keyFile.toString, "-o", z3sDec.toString, z3sEnc.toString)
        decrypted2 <- IO.blocking(Files.readAllBytes(z3sDec))
      yield forAll(List(decrypted, decrypted2))(_ should === (plain))
    }
  }
