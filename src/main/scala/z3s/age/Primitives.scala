package z3s.age

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.security.{GeneralSecurityException, KeyFactory, MessageDigest}
import java.security.spec.{NamedParameterSpec, XECPrivateKeySpec, XECPublicKeySpec}
import java.util.Base64
import javax.crypto.{Cipher, KeyAgreement, Mac}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

/** ageが使う暗号プリミティブ。JCAにないものはここで実装する */
private[age] object Primitives:
  private val P = BigInteger.TWO.pow(255).subtract(BigInteger.valueOf(19))
  val Basepoint: Array[Byte] = Array.tabulate(32)(i => if i == 0 then 9.toByte else 0.toByte)

  extension (a: IArray[Byte]) def bytes: Array[Byte] = IArray.genericWrapArray(a).toArray

  /** RFC 7748のX25519。共有秘密が全ゼロ（低位数の点）の場合はLeft */
  def x25519(scalar: Array[Byte], u: Array[Byte]): Either[String, Array[Byte]] =
    if scalar.length != 32 || u.length != 32 then Left("X25519 inputs must be 32 bytes")
    else
      try
        val kf = KeyFactory.getInstance("XDH")
        val priv = kf.generatePrivate(XECPrivateKeySpec(NamedParameterSpec.X25519, scalar.clone()))
        val pub = kf.generatePublic(XECPublicKeySpec(NamedParameterSpec.X25519, decodeU(u)))
        val ka = KeyAgreement.getInstance("XDH")
        ka.init(priv)
        ka.doPhase(pub, true)
        val shared = ka.generateSecret()
        if shared.forall(_ == 0) then Left("X25519 shared secret is all zero") else Right(shared)
      catch
        // JCAは低位数の点をInvalidKeyException / IllegalStateExceptionで拒否する
        case e: (GeneralSecurityException | IllegalStateException) => Left(s"X25519 failed: ${e.getMessage}")

  // u座標はリトルエンディアン。最上位ビットを捨て、非正規な値はmod pで扱う（RFC 7748 5章）
  private def decodeU(u: Array[Byte]): BigInteger =
    val be = u.reverse
    be(0) = (be(0) & 0x7f).toByte
    BigInteger(1, be).mod(P)

  def hmacSha256(key: Array[Byte], data: Array[Byte]): Array[Byte] =
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(key, "HmacSHA256"))
    mac.doFinal(data)

  /** RFC 5869のHKDF-SHA-256。ageは32バイトの出力しか使わない */
  def hkdf(ikm: Array[Byte], salt: Array[Byte], info: String): Array[Byte] =
    // 空のsaltはHashLen個のゼロと等価。SecretKeySpecは空の鍵を受け付けないため置き換える
    val prk = hmacSha256(if salt.isEmpty then new Array[Byte](32) else salt, ikm)
    hmacSha256(prk, info.getBytes(StandardCharsets.US_ASCII) :+ 1.toByte)

  // JCAのCipherは同じkey+nonceでの再initを拒否するため、呼び出しごとに作る
  private def chacha(mode: Int, key: Array[Byte], nonce: Array[Byte], data: Array[Byte]): Array[Byte] =
    val cipher = Cipher.getInstance("ChaCha20-Poly1305")
    cipher.init(mode, SecretKeySpec(key, "ChaCha20"), IvParameterSpec(nonce))
    cipher.doFinal(data)

  def seal(key: Array[Byte], nonce: Array[Byte], plaintext: Array[Byte]): Array[Byte] =
    chacha(Cipher.ENCRYPT_MODE, key, nonce, plaintext)

  /** 認証に失敗した場合はNone */
  def open(key: Array[Byte], nonce: Array[Byte], ciphertext: Array[Byte]): Option[Array[Byte]] =
    try Some(chacha(Cipher.DECRYPT_MODE, key, nonce, ciphertext))
    catch case _: GeneralSecurityException => None

  def constantTimeEquals(a: Array[Byte], b: Array[Byte]): Boolean = MessageDigest.isEqual(a, b)

  def base64Encode(bytes: Array[Byte]): String = Base64.getEncoder.withoutPadding.encodeToString(bytes)

  /** パディングなしの標準base64を厳密に復号する。'='や未使用ビットが0でない非正規な表現は拒否する */
  def base64Decode(s: String): Either[String, Array[Byte]] =
    if s.contains('=') then Left("base64 padding is not allowed")
    else if s.exists(c => c == '\r' || c == '\n') then Left("base64 must not contain line breaks")
    else
      try
        val bytes = Base64.getDecoder.decode(s)
        if base64Encode(bytes) == s then Right(bytes) else Left("non-canonical base64")
      catch case _: IllegalArgumentException => Left("invalid base64")
