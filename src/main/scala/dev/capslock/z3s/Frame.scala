package dev.capslock.z3s

import cats.effect.IO
import fs2.{Chunk, Pull, Stream}
import fs2.interop.scodec.StreamDecoder
import scodec.Codec
import scodec.bits.*
import scodec.codecs.*

final case class Frame(version: Int, header: ByteVector)

object Frame:
  val Magic: ByteVector = hex"5a33534255500000"
  val CurrentVersion = 1
  val MaxHeaderLength = 65536L
  /** magicとversionとヘッダ長を合わせたバイト数 */
  val PrefixLength = 14
  val MaxFrameLength: Int = PrefixLength + MaxHeaderLength.toInt

  // ヘッダ長の上限をデコード時に検査する。上限を超える値を読んだ時点で失敗させ、
  // 不正なオブジェクトのヘッダをメモリに読み込み続けないようにする
  private val headerLength: Codec[Long] =
    uint32.exmap(
      n =>
        if n <= MaxHeaderLength then scodec.Attempt.successful(n)
        else scodec.Attempt.failure(scodec.Err(s"header length $n exceeds $MaxHeaderLength")),
      n => scodec.Attempt.successful(n)
    )

  val codec: Codec[Frame] =
    (constant(Magic) ~> (uint16 :: variableSizeBytesLong(headerLength, bytes))).as[Frame]

  def encode(frame: Frame): ByteVector = codec.encode(frame).require.bytes

  /** 先頭のフレームをデコードし、フレームと残りのpayloadを`f`に渡す */
  def split[O](in: Stream[IO, Byte])(f: (Frame, Stream[IO, Byte]) => Stream[IO, O]): Stream[IO, O] =
    val decoder: StreamDecoder[Either[Frame, BitVector]] =
      StreamDecoder.once(codec).flatMap { frame =>
        StreamDecoder.emit(Left(frame)) ++ StreamDecoder.many(scodec.codecs.bits).map(Right(_))
      }
    decoder
      .decode[IO](in.chunks.map(_.toByteVector.bits))
      .pull
      .uncons1
      .flatMap {
        case Some((Left(frame), tl)) =>
          val payload = tl.flatMap {
            case Right(b) => Stream.chunk(Chunk.byteVector(b.bytes))
            case Left(_)  => Stream.raiseError[IO](Z3SError.Runtime("unexpected second frame"))
          }
          f(frame, payload).pull.echo
        case _ => Pull.raiseError[IO](Z3SError.Runtime("object is too short to contain a frame"))
      }
      .stream
