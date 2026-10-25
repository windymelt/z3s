package dev.capslock.z3s

import cats.effect.unsafe.implicits.global
import java.security.MessageDigest
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector

class MultipartUploaderSuite extends UnitSpec, ScalaCheckPropertyChecks:
  private val MiB = 1024 * 1024

  /** 分割の境界の前後を多めに選ぶ */
  private val sizes: Gen[Int] = Gen.frequency(
    1 -> Gen.choose(0, 4 * MiB),
    1 -> Gen.choose(0, 4).flatMap(n => Gen.choose(-1, 1).map(d => math.max(0, n * MiB + d)))
  )

  private val parts: Gen[Array[Byte]] =
    for
      size <- sizes
      seed <- Gen.long
    yield
      val bytes = new Array[Byte](size)
      scala.util.Random(seed).nextBytes(bytes)
      bytes

  test("the MD5 computed in slices matches the MD5 of the whole part") {
    forAll(parts, minSuccessful(30)) { bytes =>
      val expected = ByteVector(MessageDigest.getInstance("MD5").digest(bytes)).toHex
      MultipartUploader.md5Hex(bytes).unsafeRunSync() should === (expected)
    }
  }
