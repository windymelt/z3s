package dev.capslock.z3s.age

import munit.CatsEffectSuite

class KeysSuite extends CatsEffectSuite:
  test("BIP 173 bech32 test vectors are accepted") {
    List(
      "A12UEL5L",
      "a12uel5l",
      "an83characterlonghumanreadablepartthatcontainsthenumber1andtheexcludedcharactersbio1tt5tgs",
      "abcdef1qpzry9x8gf2tvdw0s3jn54khce6mua7lmqqqxw",
      "split1checkupstagehandshakeupstreamerranterredcaperred2y9e3w"
    ).foreach(s => assert(Bech32.decode(s).isRight, s))
  }

  test("invalid bech32 strings are rejected") {
    List(
      "a12uel5m", // チェックサム誤り
      "A12uEL5L", // 大文字小文字の混在
      "a1lqfn3a", // bech32mのチェックサム
      "pzry9x0s0muk", // 区切り文字なし
      "1pzry9x0s0muk", // hrpが空
      "x1b4n0q5v", // データ部に不正な文字
      "li1dgmt3" // チェックサムが短すぎる
    ).foreach(s => assert(Bech32.decode(s).isLeft, s))
  }

  test("encode and parse round trip") {
    for id <- X25519Identity.generate
    yield
      val sk = id.encode
      assert(sk.startsWith("AGE-SECRET-KEY-1"), sk)
      assertEquals(sk, sk.toUpperCase)
      assertEquals(X25519Identity.parse(sk), Right(id))
      val pk = id.recipient.encode
      assert(pk.startsWith("age1"), pk)
      assertEquals(X25519Recipient.parse(pk), Right(id.recipient))
      assertEquals(X25519Recipient.parse(pk).map(_.hashCode), Right(id.recipient.hashCode))
  }

  test("checksum errors and wrong prefixes are rejected") {
    for id <- X25519Identity.generate
    yield
      val pk = id.recipient.encode
      val flipped = pk.dropRight(1) + (if pk.last == 'q' then 'p' else 'q')
      assert(X25519Recipient.parse(flipped).isLeft)
      val sk = id.encode
      val skFlipped = sk.dropRight(1) + (if sk.last == 'Q' then 'P' else 'Q')
      assert(X25519Identity.parse(skFlipped).isLeft)
      // identityは大文字、recipientは小文字のhrpだけを受け付ける
      assert(X25519Identity.parse(sk.toLowerCase).isLeft)
      assert(X25519Recipient.parse(pk.toUpperCase).isLeft)
      assert(X25519Recipient.parse(sk).isLeft)
      assert(X25519Identity.parse(pk).isLeft)
  }

  test("toString does not reveal the secret key") {
    for id <- X25519Identity.generate
    yield
      assert(!id.toString.contains(id.encode))
      assert(!id.toString.toUpperCase.contains(id.encode.drop(16)))
  }

  test("parseFile ignores comments and blank lines") {
    for
      a <- X25519Identity.generate
      b <- X25519Identity.generate
    yield
      val contents =
        s"""# created: 2026-10-23T00:00:00+09:00
           |# public key: ${a.recipient.encode}
           |${a.encode}
           |
           |# public key: ${b.recipient.encode}
           |${b.encode}
           |""".stripMargin
      assertEquals(X25519Identity.parseFile(contents).map(_.toList), Right(List(a, b)))
      assert(X25519Identity.parseFile("# only comments\n\n").isLeft)
      assert(X25519Identity.parseFile(s"${a.encode}\nnot-a-key\n").left.exists(_.startsWith("line 2:")))
  }
