package dev.capslock.z3s.age

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import fs2.{Chunk, Pipe, Pull, Stream}

import java.security.SecureRandom

import Primitives.bytes

final class AgeException(message: String, cause: Throwable = null) extends Exception(message, cause)

/** age v1形式（https://github.com/C2SP/C2SP/blob/main/age.md）のX25519 recipientによる暗号化・復号 */
object Age:
  private val X25519Label = "age-encryption.org/v1/X25519"
  private val FileKeySize = 16
  private val NonceSize = 16
  private val TagSize = 16
  private val PlainChunkSize = 64 * 1024
  private val CipherChunkSize = PlainChunkSize + TagSize
  /** ヘッダとして読み込む最大バイト数。超えた時点で失敗させる */
  val MaxHeaderSize = 64 * 1024

  private val ZeroNonce = new Array[Byte](12)

  private def fail(message: String): Pull[IO, Nothing, Nothing] = Pull.raiseError[IO](AgeException(message))

  private def headerMac(fileKey: Array[Byte], macInput: Array[Byte]): Array[Byte] =
    Primitives.hmacSha256(Primitives.hkdf(fileKey, Array.emptyByteArray, "header"), macInput)

  // nonceの先頭11バイトはビッグエンディアンのチャンクカウンタ、最終バイトは最終チャンクのみ1
  private[age] def chunkNonce(counter: Long, last: Boolean): Array[Byte] =
    val nonce = new Array[Byte](12)
    for i <- 0 until 8 do nonce(10 - i) = (counter >>> (8 * i)).toByte
    if last then nonce(11) = 1
    nonce

  private[age] def wrap(recipient: X25519Recipient, fileKey: Array[Byte], random: SecureRandom): Stanza =
    val ephemeral = new Array[Byte](32)
    random.nextBytes(ephemeral)
    val share = Primitives.x25519(ephemeral, Primitives.Basepoint).fold(e => throw AgeException(e), identity)
    val pub = recipient.publicKey.bytes
    val shared =
      Primitives.x25519(ephemeral, pub).fold(e => throw AgeException(s"invalid recipient ${recipient.encode}: $e"), identity)
    val wrapKey = Primitives.hkdf(shared, share ++ pub, X25519Label)
    Stanza(List("X25519", Primitives.base64Encode(share)), Primitives.seal(wrapKey, ZeroNonce, fileKey))

  def encrypt(recipients: NonEmptyList[X25519Recipient]): Pipe[IO, Byte, Byte] = in =>
    Stream
      .eval(IO {
        val random = SecureRandom()
        val fileKey = new Array[Byte](FileKeySize)
        random.nextBytes(fileKey)
        val headerNoMac = Header.encodeWithoutMac(recipients.toList.map(wrap(_, fileKey, random)))
        val macLine = s" ${Primitives.base64Encode(headerMac(fileKey, headerNoMac))}\n".getBytes("US-ASCII")
        // 復号側の上限を超えるヘッダは書かない。書くと復元できないバックアップになる
        if headerNoMac.length + macLine.length > MaxHeaderSize then
          throw AgeException(s"age header exceeds $MaxHeaderSize bytes (${recipients.size} recipients)")
        val nonce = new Array[Byte](NonceSize)
        random.nextBytes(nonce)
        (headerNoMac ++ macLine ++ nonce, Primitives.hkdf(fileKey, nonce, "payload"))
      })
      .flatMap { (prefix, payloadKey) =>
        Stream.chunk(Chunk.array(prefix)) ++ encryptChunks(in, payloadKey, 0L).stream
      }

  private def encryptChunks(s: Stream[IO, Byte], key: Array[Byte], counter: Long): Pull[IO, Byte, Unit] =
    def seal(plain: Chunk[Byte], last: Boolean): Pull[IO, Byte, Unit] =
      Pull.eval(IO(Primitives.seal(key, chunkNonce(counter, last), plain.toArray))).flatMap(c => Pull.output(Chunk.array(c)))

    s.pull.unconsN(PlainChunkSize, allowFewer = true).flatMap {
      // 平文全体が空の場合のみ、空の最終チャンクを出力する
      case None => seal(Chunk.empty, last = true)
      case Some((plain, _)) if plain.size < PlainChunkSize => seal(plain, last = true)
      case Some((plain, tail)) =>
        tail.pull.peek.flatMap {
          case None             => seal(plain, last = true)
          case Some((_, tail2)) => seal(plain, last = false) >> encryptChunks(tail2, key, counter + 1)
        }
    }

  def decrypt(identities: NonEmptyList[X25519Identity]): Pipe[IO, Byte, Byte] = in =>
    (for
      (raw, rest) <- readHeader(in, Chunk.empty, 0)
      header <- Pull.eval(IO.fromEither(Header.parse(raw.toArray).leftMap(e => AgeException(s"invalid age header: $e"))))
      fileKey <- Pull.eval(IO.fromEither(unwrap(header, identities).leftMap(AgeException(_))))
      _ <-
        if Primitives.constantTimeEquals(headerMac(fileKey, header.macInput), header.mac) then Pull.done
        else fail("header MAC mismatch")
      nonceAndRest <- rest.pull.unconsN(NonceSize, allowFewer = false)
      _ <- nonceAndRest match
        case None => fail("truncated payload nonce")
        case Some((nonce, payload)) =>
          decryptChunks(payload, Primitives.hkdf(fileKey, nonce.toArray, "payload"), 0L)
    yield ()).stream

  private val Terminator = "\n---".getBytes("US-ASCII")

  /** "\n---"で始まる行の改行までをヘッダとして切り出す。matchedはTerminatorの一致済み長、
    * 4ならMAC行の改行待ちを表す
    */
  private def readHeader(
      s: Stream[IO, Byte],
      acc: Chunk[Byte],
      matched: Int
  ): Pull[IO, Nothing, (Chunk[Byte], Stream[IO, Byte])] =
    s.pull.uncons.flatMap {
      case None => fail("truncated age header")
      case Some((chunk, tail)) =>
        var m = matched
        var end = -1
        var i = 0
        while end < 0 && i < chunk.size do
          val b = chunk(i)
          if m == Terminator.length then { if b == '\n' then end = i + 1 }
          else if b == Terminator(m) then m += 1
          else m = if b == '\n' then 1 else 0
          i += 1
        if end >= 0 then
          val (head, rest) = chunk.splitAt(end)
          if acc.size + head.size > MaxHeaderSize then fail(s"age header exceeds $MaxHeaderSize bytes")
          else Pull.pure((acc ++ head, Stream.chunk(rest) ++ tail))
        else if acc.size + chunk.size > MaxHeaderSize then fail(s"age header exceeds $MaxHeaderSize bytes")
        else readHeader(tail, acc ++ chunk, m)
    }

  private def unwrap(header: Header, identities: NonEmptyList[X25519Identity]): Either[String, Array[Byte]] =
    val stanzas = header.stanzas.filter(_.args.headOption.contains("X25519"))
    // X25519 stanzaの形式誤りは無視せずエラーにする
    stanzas
      .traverse { stanza =>
        stanza.args match
          case List(_, shareText) =>
            for
              share <- Primitives.base64Decode(shareText).leftMap(e => s"invalid X25519 share: $e")
              _ <- Either.cond(share.length == 32, (), "invalid X25519 share length")
              _ <- Either.cond(stanza.body.length == FileKeySize + TagSize, (), "invalid X25519 stanza body length")
            yield (share, stanza.body)
          case _ => Left("invalid X25519 stanza")
      }
      .flatMap { parsed =>
        val attempts =
          for
            (share, body) <- parsed.iterator
            identity <- identities.iterator
          yield Primitives.x25519(identity.secretKey.bytes, share).map { shared =>
            val wrapKey = Primitives.hkdf(shared, share ++ identity.recipient.publicKey.bytes, X25519Label)
            Primitives.open(wrapKey, ZeroNonce, body)
          }
        // 最初に復号できたfile key、または最初のエラーを結果とする
        attempts
          .collectFirst {
            case Left(e)        => Left(s"invalid X25519 stanza: $e")
            case Right(Some(k)) => Right(k)
          }
          .getOrElse(Left("no identity matched any of the recipients"))
      }

  private def decryptChunks(s: Stream[IO, Byte], key: Array[Byte], counter: Long): Pull[IO, Byte, Unit] =
    def open(cipher: Chunk[Byte], last: Boolean): Pull[IO, Byte, Unit] =
      if cipher.size < TagSize then fail("truncated payload chunk")
      else
        Primitives.open(key, chunkNonce(counter, last), cipher.toArray) match
          case None => fail(s"payload chunk $counter failed authentication (corrupted or truncated)")
          case Some(plain) if last && plain.isEmpty && counter > 0 => fail("unexpected empty final chunk")
          case Some(plain) => Pull.output(Chunk.array(plain))

    s.pull.unconsN(CipherChunkSize, allowFewer = true).flatMap {
      case None => fail("truncated payload")
      case Some((cipher, _)) if cipher.size < CipherChunkSize => open(cipher, last = true)
      case Some((cipher, tail)) =>
        // 後続データの有無で最終チャンクかを判定する
        tail.pull.peek.flatMap {
          case None             => open(cipher, last = true)
          case Some((_, tail2)) => open(cipher, last = false) >> decryptChunks(tail2, key, counter + 1)
        }
    }
