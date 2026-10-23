package dev.capslock.z3s.age

import dev.capslock.z3s.IOSpec
import org.scalatest.Assertion

class KeysSuite extends IOSpec:
  test("BIP 173 bech32 test vectors are accepted") {
    forAll(
      List(
        "A12UEL5L",
        "a12uel5l",
        "an83characterlonghumanreadablepartthatcontainsthenumber1andtheexcludedcharactersbio1tt5tgs",
        "abcdef1qpzry9x8gf2tvdw0s3jn54khce6mua7lmqqqxw",
        "split1checkupstagehandshakeupstreamerranterredcaperred2y9e3w"
      )
    )(s => Bech32.decode(s) shouldBe a [Right[?, ?]]): Assertion
  }

  test("invalid bech32 strings are rejected") {
    forAll(
      List(
        "a12uel5m", // チェックサム誤り
        "A12uEL5L", // 大文字小文字の混在
        "a1lqfn3a", // bech32mのチェックサム
        "pzry9x0s0muk", // 区切り文字なし
        "1pzry9x0s0muk", // hrpが空
        "x1b4n0q5v", // データ部に不正な文字
        "li1dgmt3" // チェックサムが短すぎる
      )
    )(s => Bech32.decode(s) shouldBe a [Left[?, ?]]): Assertion
  }

  test("encode and parse round trip") {
    for id <- X25519Identity.generate
    yield
      val sk = id.encode
      sk should startWith ("AGE-SECRET-KEY-1")
      sk should === (sk.toUpperCase)
      X25519Identity.parse(sk).value should === (id)
      val pk = id.recipient.encode
      pk should startWith ("age1")
      X25519Recipient.parse(pk).value should === (id.recipient)
      X25519Recipient.parse(pk).value.hashCode should === (id.recipient.hashCode)
  }

  test("checksum errors and wrong prefixes are rejected") {
    for id <- X25519Identity.generate
    yield
      val pk = id.recipient.encode
      val flipped = pk.dropRight(1) + (if pk.last == 'q' then 'p' else 'q')
      X25519Recipient.parse(flipped) shouldBe a [Left[?, ?]]
      val sk = id.encode
      val skFlipped = sk.dropRight(1) + (if sk.last == 'Q' then 'P' else 'Q')
      X25519Identity.parse(skFlipped) shouldBe a [Left[?, ?]]
      // identityは大文字、recipientは小文字のhrpだけを受け付ける
      X25519Identity.parse(sk.toLowerCase) shouldBe a [Left[?, ?]]
      X25519Recipient.parse(pk.toUpperCase) shouldBe a [Left[?, ?]]
      X25519Recipient.parse(sk) shouldBe a [Left[?, ?]]
      X25519Identity.parse(pk) shouldBe a [Left[?, ?]]
  }

  test("toString does not reveal the secret key") {
    for id <- X25519Identity.generate
    yield
      id.toString should not include (id.encode)
      id.toString.toUpperCase should not include (id.encode.drop(16))
  }

  test("parseFile ignores comments and blank lines") {
    for
      id1 <- X25519Identity.generate
      id2 <- X25519Identity.generate
    yield
      val contents =
        s"""# created: 2026-10-23T00:00:00+09:00
           |# public key: ${id1.recipient.encode}
           |${id1.encode}
           |
           |# public key: ${id2.recipient.encode}
           |${id2.encode}
           |""".stripMargin
      X25519Identity.parseFile(contents).value.toList should === (List(id1, id2))
      X25519Identity.parseFile("# only comments\n\n") shouldBe a [Left[?, ?]]
      X25519Identity.parseFile(s"${id1.encode}\nnot-a-key\n").left.value should startWith ("line 2:")
  }
