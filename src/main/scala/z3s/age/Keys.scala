package z3s.age

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*

import java.security.SecureRandom

import Primitives.bytes

final case class X25519Recipient(publicKey: IArray[Byte]):
  require(publicKey.length == 32, "X25519 public key must be 32 bytes")

  def encode: String = Bech32.encode(X25519Recipient.Hrp, publicKey.bytes)

  // IArrayの既定の比較は参照比較になるため、内容で比較する
  override def equals(that: Any): Boolean = that match
    case r: X25519Recipient => publicKey.sameElements(r.publicKey)
    case _                  => false
  override def hashCode: Int = publicKey.toSeq.hashCode
  override def toString: String = s"X25519Recipient($encode)"

object X25519Recipient:
  private val Hrp = "age"

  def parse(s: String): Either[String, X25519Recipient] =
    Bech32.decode(s).leftMap(e => s"invalid recipient: $e").flatMap {
      case (Hrp, key) if key.length == 32 => Right(X25519Recipient(IArray.unsafeFromArray(key)))
      case (Hrp, key)                     => Left(s"invalid recipient: key must be 32 bytes, got ${key.length}")
      case (hrp, _)                       => Left(s"invalid recipient: unexpected prefix '$hrp'")
    }

final case class X25519Identity private (secretKey: IArray[Byte]):
  lazy val recipient: X25519Recipient =
    val pub = Primitives
      .x25519(secretKey.bytes, Primitives.Basepoint)
      .fold(e => throw AgeException(s"failed to derive public key: $e"), identity)
    X25519Recipient(IArray.unsafeFromArray(pub))

  def encode: String = Bech32.encode(X25519Identity.Hrp, secretKey.bytes).toUpperCase

  override def equals(that: Any): Boolean = that match
    case i: X25519Identity => secretKey.sameElements(i.secretKey)
    case _                 => false
  override def hashCode: Int = secretKey.toSeq.hashCode
  // 秘密鍵をログに出さないよう、公開鍵だけを表示する
  override def toString: String = s"X25519Identity(${recipient.encode})"

object X25519Identity:
  private val Hrp = "AGE-SECRET-KEY-"

  def fromSecretKey(bytes: Array[Byte]): Either[String, X25519Identity] =
    if bytes.length == 32 then Right(new X25519Identity(IArray.from(bytes)))
    else Left(s"invalid identity: key must be 32 bytes, got ${bytes.length}")

  def parse(s: String): Either[String, X25519Identity] =
    Bech32.decode(s).leftMap(e => s"invalid identity: $e").flatMap {
      case (Hrp, key) => fromSecretKey(key)
      case (hrp, _)   => Left(s"invalid identity: unexpected prefix '$hrp'")
    }

  def generate: IO[X25519Identity] =
    IO {
      val bytes = new Array[Byte](32)
      SecureRandom().nextBytes(bytes)
      new X25519Identity(IArray.unsafeFromArray(bytes))
    }

  /** age-keygenが出力するファイル形式。'#'で始まる行と空行を無視し、残りの各行をidentityとして解釈する */
  def parseFile(contents: String): Either[String, NonEmptyList[X25519Identity]] =
    contents.linesIterator.zipWithIndex
      .map((line, i) => (line.strip, i + 1))
      .filterNot((line, _) => line.isEmpty || line.startsWith("#"))
      .toList
      .traverse((line, n) => parse(line).leftMap(e => s"line $n: $e"))
      .flatMap(NonEmptyList.fromList(_).toRight("no identities found"))
