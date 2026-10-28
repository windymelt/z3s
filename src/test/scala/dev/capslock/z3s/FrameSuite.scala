package dev.capslock.z3s

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.{Chunk, Stream}
import io.bullet.borer.{Cbor as BorerCbor, Dom}
import org.scalatest.Assertion
import scodec.bits.*

class FrameSuite extends IOSpec:
  val header = Header(
    snapshot = "tank/data@daily-2026-10-23",
    guid = -1234567890123L, // 符号なしでは2^64 - 1234567890123
    createtxg = 42,
    parent = Some(ParentRef("tank/data@daily-2026-10-22", 0x7fffffffffffffffL)),
    mode = SendMode.Plain(Compression.Zstd(3), Encryption.Age(NonEmptyList.of("age1aaa", "age1bbb"))),
    estimatedSize = 123456789,
    createdAt = "2026-10-23T00:00:00Z",
    toolVersion = "0.1.0"
  )

  test("header round trip for every mode") {
    val modes = List(
      SendMode.Raw,
      SendMode.Plain(Compression.None, Encryption.None),
      SendMode.Plain(Compression.Gzip(6), Encryption.None),
      SendMode.Plain(Compression.Xz(0), Encryption.None),
      SendMode.Plain(Compression.Bzip2(9), Encryption.None),
      header.mode
    )
    val headers =
      for mode <- modes; parent <- List(None, header.parent); partSize <- List(None, Some(5L * 1024 * 1024))
      yield header.copy(mode = mode, parent = parent, partSize = partSize)
    forAll(headers)(h => Cbor.decodeHeader(Cbor.encodeHeader(h)) should === (Right(h))): Assertion
  }

  test("guid is encoded as CBOR uint64") {
    val bytes = ByteVector(Cbor.encodeHeader(header.copy(parent = None)))
    // "guid"の直後がmajor type 0, 8バイト長(0x1b)の2^64 - 1234567890123
    val guid = hex"1b" ++ ByteVector.fromLong(-1234567890123L)
    bytes.toHex should include((ByteVector("guid".getBytes) ++ guid).toHex)
  }

  test("unknown keys are ignored") {
    val known = Cbor.encodeHeader(header)
    // 既知のmapに、入れ子のmapと配列を値に持つ未知のキーを加える
    val extra = BorerCbor
      .encode(Map[String, Map[String, List[Int]]]("future" -> Map("nested" -> List(1, 2, 3)), "note" -> Map.empty))
      .toByteArray
    val count = known(0) & 0x1f
    val merged = Array((0xa0 | (count + 2)).toByte) ++ known.drop(1) ++ extra.drop(1)
    Cbor.decodeHeader(merged) should === (Right(header))
  }

  test("part digests round trip, and an MD5 of the wrong length is rejected") {
    val digests = PartDigests(5L * 1024 * 1024, 12L * 1024 * 1024, List("00" * 16, "0123456789abcdef" * 2, "ff" * 16))
    val bytes = Cbor.encodePartDigests(digests)
    Cbor.decodePartDigests(bytes) should === (Right(digests))
    // MD5を15バイトにした一覧
    val shortened = BorerCbor
      .encode(Map[String, Dom.Element](
        "schemaVersion" -> Dom.IntElem(1),
        "partSize" -> Dom.IntElem(1),
        "size" -> Dom.IntElem(1),
        "md5" -> Dom.ArrayElem.Sized(Dom.ByteArrayElem(new Array[Byte](15)))
      ))
      .toByteArray
    Cbor.decodePartDigests(shortened).left.value should include ("MD5 must be 16 bytes, got 15")
  }

  test("frame round trip") {
    val frame = Frame(1, ByteVector(Cbor.encodeHeader(header)))
    val bytes = Frame.encode(frame)
    bytes.take(8) should === (Frame.Magic)
    bytes.slice(8, 10) should === (hex"0001")
    bytes.slice(10, 14).toLong(signed = false) should === (frame.header.size)
    Frame.codec.decode(bytes.bits).require.value should === (frame)
  }

  test("split returns the frame and the rest of the stream") {
    val frame = Frame(1, ByteVector(Cbor.encodeHeader(header)))
    val payload = Array.tabulate[Byte](300000)(i => (i * 7).toByte)
    val in = (Stream.chunk(Chunk.byteVector(Frame.encode(frame))) ++ Stream.chunk(Chunk.array(payload)))
      .covary[IO]
      .rechunkRandomlyWithSeed(0.01, 3.0)(1)
    Frame
      .split(in)((f, rest) => Stream.emit(f).covary[IO].map(Left(_)) ++ rest.chunks.map(Right(_)))
      .compile
      .toList
      .map { out =>
        out.head should === (Left(frame))
        ByteVector(out.tail.collect { case Right(c) => c.toArray }.flatten) should === (ByteVector(payload))
      }
  }

  test("split rejects a wrong magic and an oversized header length") {
    val badMagic = Stream.chunk(Chunk.byteVector(hex"0000000000000000" ++ hex"0001" ++ hex"00000000")).covary[IO]
    val tooLong = Stream.chunk(Chunk.byteVector(Frame.Magic ++ hex"0001" ++ hex"00010001")).covary[IO]
    for
      wrongMagic <- Frame.split(badMagic)((_, rest) => rest).compile.drain.attempt
      oversized <- Frame.split(tooLong)((_, rest) => rest).compile.drain.attempt
      truncated <- Frame.split(Stream.emits(Frame.Magic.toArray).covary[IO])((_, rest) => rest).compile.drain.attempt
    yield forAll(List(wrongMagic, oversized, truncated))(_ shouldBe a[Left[?, ?]])
  }

  test("compression option parsing") {
    Compression.parse("zstd") should === (Right(Compression.Zstd(3)))
    Compression.parse("gzip:9") should === (Right(Compression.Gzip(9)))
    Compression.parse("xz:0") should === (Right(Compression.Xz(0)))
    Compression.parse("bzip2") should === (Right(Compression.Bzip2(9)))
    Compression.parse("none") should === (Right(Compression.None))
    forAll(List("zstd:20", "gzip:0", "lz4", "none:1"))(s => Compression.parse(s) shouldBe a[Left[?, ?]]): Assertion
  }
