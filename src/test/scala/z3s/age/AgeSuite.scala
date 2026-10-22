package z3s.age

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.security.SecureRandom

import AgeTestSupport.*
import Primitives.bytes

class AgeSuite extends CatsEffectSuite:
  private val K = 64 * 1024
  private val CipherChunk = K + 16

  private def roundTrip(size: Int, inChunk: Int = 8192, outChunk: Int = 8192)(using munit.Location): IO[Unit] =
    for
      id <- X25519Identity.generate
      plain = randomBytes(size, size.toLong)
      cipher <- encryptBytes(NonEmptyList.one(id.recipient), plain, inChunk)
      decrypted <- decryptBytes(NonEmptyList.one(id), cipher, outChunk)
    yield assert(decrypted.sameElements(plain), s"round trip failed for size $size")

  List(0, 1, 100, K - 1, K, K + 1, 3 * K, 4 * 1024 * 1024 + 123).foreach { size =>
    test(s"round trip $size bytes")(roundTrip(size))
  }

  test("ciphertext size follows the chunk layout") {
    for
      id <- X25519Identity.generate
      sizes = List(0 -> 1, 1 -> 1, K -> 1, K + 1 -> 2, 3 * K -> 3)
      results <- sizes.traverse { (size, chunks) =>
        encryptBytes(NonEmptyList.one(id.recipient), randomBytes(size)).map(c => (c.length - headerLength(c), size, chunks))
      }
    yield results.foreach { (payload, size, chunks) =>
      assertEquals(payload, 16 + size + 16 * chunks, s"size $size")
    }
  }

  test("fine-grained input chunking gives the same result") {
    for
      _ <- roundTrip(1000, inChunk = 1, outChunk = 1)
      _ <- roundTrip(3 * K + 5, inChunk = 7, outChunk = 13)
      _ <- roundTrip(2 * K, inChunk = 1000, outChunk = 1)
    yield ()
  }

  test("multiple recipients: any identity can decrypt") {
    for
      ids <- List.fill(3)(X25519Identity.generate).sequence
      plain = randomBytes(200000)
      cipher <- encryptBytes(NonEmptyList.fromListUnsafe(ids.map(_.recipient)), plain)
      bySecond <- decryptBytes(NonEmptyList.one(ids(1)), cipher)
      other <- X25519Identity.generate
      byList <- decryptBytes(NonEmptyList.of(other, ids(2)), cipher)
    yield
      assert(bySecond.sameElements(plain))
      assert(byList.sameElements(plain))
  }

  test("header format: stanza lines and MAC line") {
    for
      id <- X25519Identity.generate
      cipher <- encryptBytes(NonEmptyList.of(id.recipient, id.recipient), Array.emptyByteArray)
    yield
      val header = String(cipher.take(headerLength(cipher)), StandardCharsets.US_ASCII)
      val lines = header.split("\n").toList
      assertEquals(lines.head, "age-encryption.org/v1")
      assertEquals(lines.length, 6)
      List(1, 3).foreach { i =>
        assert(lines(i).matches("-> X25519 [A-Za-z0-9+/]{43}"), lines(i))
        assert(lines(i + 1).matches("[A-Za-z0-9+/]{43}"), lines(i + 1))
      }
      assert(lines(5).matches("--- [A-Za-z0-9+/]{43}"), lines(5))
  }

  test("stanza body wraps at 64 columns") {
    val body48 = Header.encodeStanza(Stanza(List("t"), new Array[Byte](48)))
    assertEquals(body48, "-> t\n" + "A" * 64 + "\n\n")
    val body50 = Header.encodeStanza(Stanza(List("t"), new Array[Byte](50)))
    assertEquals(body50, "-> t\n" + "A" * 64 + "\nAAA\n")
  }

  test("wrong identity fails") {
    for
      id <- X25519Identity.generate
      other <- X25519Identity.generate
      cipher <- encryptBytes(NonEmptyList.one(id.recipient), randomBytes(100))
      _ <- interceptIO[AgeException](decryptBytes(NonEmptyList.one(other), cipher))
    yield ()
  }

  // チャンク分割の違いで結果が変わらないことも確かめる
  private def expectFailure(cipher: Array[Byte], id: X25519Identity)(using munit.Location): IO[Unit] =
    interceptIO[AgeException](decryptBytes(NonEmptyList.one(id), cipher, 1000)) >>
      interceptIO[AgeException](decryptBytes(NonEmptyList.one(id), cipher, 7)).void

  /** file keyとstanza、payloadのチャンク（平文と最終フラグ）から暗号文を手で組み立てる */
  private def assemble(fileKey: Array[Byte], stanzas: List[Stanza], chunks: List[(Array[Byte], Boolean)]): Array[Byte] =
    val headerNoMac = Header.encodeWithoutMac(stanzas)
    val mac = Primitives.hmacSha256(Primitives.hkdf(fileKey, Array.emptyByteArray, "header"), headerNoMac)
    val nonce = randomBytes(16, 7L)
    val key = Primitives.hkdf(fileKey, nonce, "payload")
    val payload = chunks.zipWithIndex.flatMap { case ((plain, last), i) =>
      Primitives.seal(key, Age.chunkNonce(i.toLong, last), plain).toList
    }
    headerNoMac ++ s" ${Primitives.base64Encode(mac)}\n".getBytes("US-ASCII") ++ nonce ++ payload

  private def withCipher(size: Int)(f: (X25519Identity, Array[Byte]) => IO[Unit]): IO[Unit] =
    for
      id <- X25519Identity.generate
      cipher <- encryptBytes(NonEmptyList.one(id.recipient), randomBytes(size))
      _ <- f(id, cipher)
    yield ()

  test("truncation is detected") {
    withCipher(3 * K) { (id, cipher) =>
      val h = headerLength(cipher)
      List(
        h / 2, // ヘッダ途中
        h - 1, // MAC行の改行の直前
        h, // nonceなし
        h + 10, // nonce途中
        h + 16, // チャンクなし
        h + 16 + 1000, // payload途中
        h + 16 + CipherChunk, // チャンク境界
        h + 16 + 2 * CipherChunk, // チャンク境界
        cipher.length - 1
      ).traverse_(n => expectFailure(cipher.take(n), id))
    }
  }

  test("truncation of a single full final chunk is detected") {
    withCipher(K) { (id, cipher) =>
      List(headerLength(cipher) + 16 + 15, cipher.length - 16).traverse_(n => expectFailure(cipher.take(n), id))
    }
  }

  test("modification is detected") {
    withCipher(2 * K + 10) { (id, cipher) =>
      val h = headerLength(cipher)
      val positions = List(
        5, // バージョン行
        25, // stanza引数
        h - 30, // MAC
        h - 50, // stanza body
        h + 3, // payload nonce
        h + 16 + 5, // 1番目のチャンク
        h + 16 + CipherChunk + 100, // 2番目のチャンク
        cipher.length - 1 // 最終チャンクのタグ
      )
      positions.traverse_ { pos =>
        val tampered = cipher.clone()
        tampered(pos) = (tampered(pos) ^ 0x01).toByte
        expectFailure(tampered, id)
      }
    }
  }

  test("trailing data is rejected") {
    List(0, 100, K).traverse_ { size =>
      withCipher(size) { (id, cipher) =>
        List(1, 16, 17, CipherChunk, CipherChunk + 1).traverse_ { extra =>
          expectFailure(cipher ++ randomBytes(extra, extra.toLong), id)
        }
      }
    }
  }

  test("header size is limited") {
    for
      id <- X25519Identity.generate
      junk = ("age-encryption.org/v1\n" + "-> x\n" + ("A" * 63 + "\n") * 2000).getBytes("US-ASCII")
      _ <- interceptMessageIO[AgeException](s"age header exceeds ${Age.MaxHeaderSize} bytes")(
        decryptBytes(NonEmptyList.one(id), junk)
      )
    yield ()
  }

  test("non-canonical base64 and padding are rejected") {
    assert(Primitives.base64Decode("AAAAAAAAAAAAAAAAAAAAAA").isRight)
    assert(Primitives.base64Decode("AAAAAAAAAAAAAAAAAAAAAA==").isLeft)
    assert(Primitives.base64Decode("AAAAAAAAAAAAAAAAAAAAAB").isLeft)
    assert(Primitives.base64Decode("AAA-").isLeft)
  }

  test("stanzas of unknown types are ignored") {
    for
      id <- X25519Identity.generate
      fileKey = randomBytes(16, 1L)
      plain = randomBytes(100)
      unknown = Stanza(List("unknown", "a", "b"), randomBytes(70))
      cipher = assemble(fileKey, List(unknown, Age.wrap(id.recipient, fileKey, SecureRandom())), List(plain -> true))
      decrypted <- decryptBytes(NonEmptyList.one(id), cipher)
    yield assert(decrypted.sameElements(plain))
  }

  test("assembled ciphertext with a final chunk of exactly 64 KiB decrypts") {
    for
      id <- X25519Identity.generate
      fileKey = randomBytes(16, 2L)
      chunks = List(randomBytes(K, 3L) -> false, randomBytes(K, 4L) -> true)
      cipher = assemble(fileKey, List(Age.wrap(id.recipient, fileKey, SecureRandom())), chunks)
      decrypted <- decryptBytes(NonEmptyList.one(id), cipher)
    yield assert(decrypted.sameElements(chunks.flatMap(_._1)))
  }

  test("empty final chunk after a non-empty payload is rejected") {
    for
      id <- X25519Identity.generate
      fileKey = randomBytes(16, 5L)
      chunks = List(randomBytes(K) -> false, Array.emptyByteArray -> true)
      cipher = assemble(fileKey, List(Age.wrap(id.recipient, fileKey, SecureRandom())), chunks)
      _ <- interceptMessageIO[AgeException]("unexpected empty final chunk")(decryptBytes(NonEmptyList.one(id), cipher))
    yield ()
  }

  test("a short non-final chunk is rejected") {
    for
      id <- X25519Identity.generate
      fileKey = randomBytes(16, 6L)
      chunks = List(randomBytes(100) -> false, randomBytes(100) -> true)
      cipher = assemble(fileKey, List(Age.wrap(id.recipient, fileKey, SecureRandom())), chunks)
      _ <- expectFailure(cipher, id)
    yield ()
  }

  test("low-order ephemeral share is rejected") {
    for
      id <- X25519Identity.generate
      header = Header.encodeWithoutMac(List(Stanza(List("X25519", Primitives.base64Encode(new Array[Byte](32))), new Array[Byte](32))))
      cipher = header ++ s" ${Primitives.base64Encode(new Array[Byte](32))}\n".getBytes("US-ASCII")
      _ <- interceptIO[AgeException](decryptBytes(NonEmptyList.one(id), cipher))
    yield ()
  }

  test("public key derivation matches the X25519 shared secret") {
    for
      a <- X25519Identity.generate
      b <- X25519Identity.generate
    yield
      val ab = Primitives.x25519(a.secretKey.bytes, b.recipient.publicKey.bytes)
      val ba = Primitives.x25519(b.secretKey.bytes, a.recipient.publicKey.bytes)
      assertEquals(ab.map(_.toList), ba.map(_.toList))
  }
