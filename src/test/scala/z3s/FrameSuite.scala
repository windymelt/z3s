package z3s

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.{Chunk, Stream}
import io.bullet.borer.Cbor as BorerCbor
import munit.CatsEffectSuite
import scodec.bits.*

class FrameSuite extends CatsEffectSuite:
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
    for mode <- modes; parent <- List(None, header.parent) do
      val h = header.copy(mode = mode, parent = parent)
      assertEquals(Cbor.decodeHeader(Cbor.encodeHeader(h)), Right(h))
  }

  test("guid is encoded as CBOR uint64") {
    val bytes = ByteVector(Cbor.encodeHeader(header.copy(parent = None)))
    // "guid"の直後がmajor type 0, 8バイト長(0x1b)の2^64 - 1234567890123
    val guid = hex"1b" ++ ByteVector.fromLong(-1234567890123L)
    assert(bytes.containsSlice(ByteVector("guid".getBytes) ++ guid), bytes.toHex)
  }

  test("unknown keys are ignored") {
    val known = Cbor.encodeHeader(header)
    // 既知のmapに、入れ子のmapと配列を値に持つ未知のキーを加える
    val extra = BorerCbor
      .encode(Map[String, Map[String, List[Int]]]("future" -> Map("nested" -> List(1, 2, 3)), "note" -> Map.empty))
      .toByteArray
    val count = known(0) & 0x1f
    val merged = Array((0xa0 | (count + 2)).toByte) ++ known.drop(1) ++ extra.drop(1)
    assertEquals(Cbor.decodeHeader(merged), Right(header))
  }

  test("frame round trip") {
    val frame = Frame(1, ByteVector(Cbor.encodeHeader(header)))
    val bytes = Frame.encode(frame)
    assertEquals(bytes.take(8), Frame.Magic)
    assertEquals(bytes.slice(8, 10), hex"0001")
    assertEquals(bytes.slice(10, 14).toLong(signed = false), frame.header.size)
    assertEquals(Frame.codec.decode(bytes.bits).require.value, frame)
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
        assertEquals(out.head, Left(frame))
        assertEquals(out.tail.collect { case Right(c) => c.toArray }.flatten, payload.toList)
      }
  }

  test("split rejects a wrong magic and an oversized header length") {
    val badMagic = Stream.chunk(Chunk.byteVector(hex"0000000000000000" ++ hex"0001" ++ hex"00000000")).covary[IO]
    val tooLong = Stream.chunk(Chunk.byteVector(Frame.Magic ++ hex"0001" ++ hex"00010001")).covary[IO]
    for
      a <- Frame.split(badMagic)((_, rest) => rest).compile.drain.attempt
      b <- Frame.split(tooLong)((_, rest) => rest).compile.drain.attempt
      c <- Frame.split(Stream.emits(Frame.Magic.toArray).covary[IO])((_, rest) => rest).compile.drain.attempt
    yield
      assert(a.isLeft)
      assert(b.isLeft)
      assert(c.isLeft)
  }

  test("compression option parsing") {
    assertEquals(Compression.parse("zstd"), Right(Compression.Zstd(3)))
    assertEquals(Compression.parse("gzip:9"), Right(Compression.Gzip(9)))
    assertEquals(Compression.parse("xz:0"), Right(Compression.Xz(0)))
    assertEquals(Compression.parse("bzip2"), Right(Compression.Bzip2(9)))
    assertEquals(Compression.parse("none"), Right(Compression.None))
    assert(Compression.parse("zstd:20").isLeft)
    assert(Compression.parse("gzip:0").isLeft)
    assert(Compression.parse("lz4").isLeft)
    assert(Compression.parse("none:1").isLeft)
  }
