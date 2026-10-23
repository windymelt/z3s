package dev.capslock.z3s

import cats.effect.IO
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import scodec.bits.ByteVector

class CompressorsSuite extends IOSpec:
  val algorithms = List(
    Compression.None,
    Compression.Gzip(1),
    Compression.Gzip(9),
    Compression.Zstd(1),
    Compression.Zstd(19),
    Compression.Xz(0),
    Compression.Xz(6),
    Compression.Bzip2(1),
    Compression.Bzip2(9)
  )

  // 乱数の部分と繰り返しの部分をつなげ、圧縮しにくいデータと圧縮しやすいデータを混ぜる
  val data: Array[Byte] =
    val rnd = new scala.util.Random(0)
    Array.fill(400000)(rnd.nextInt(256).toByte) ++ Array.tabulate(600000)(i => (i % 17).toByte)

  for alg <- algorithms do
    test(s"round trip: ${alg.label}") {
      // ByteVectorで比べると、512バイト以上のデータは失敗時に中身ではなくサイズとハッシュが表示される
      List(Array.emptyByteArray, Array[Byte](1), data)
        .traverse { input =>
          Stream
            .chunk(Chunk.array(input))
            .covary[IO]
            .rechunkRandomly(0.001, 2.0)
            .through(Compressors.compress(alg))
            .rechunkRandomly(0.01, 2.0)
            .through(Compressors.decompress(alg))
            .compile
            .to(Array)
            .map(out => (ByteVector(input), ByteVector(out)))
        }
        .map(results => forAll(results)((input, out) => out should === (input)))
    }

  test("compressed data is smaller for compressible input") {
    val input = Array.tabulate[Byte](1000000)(i => (i % 17).toByte)
    algorithms
      .filter(_ != Compression.None)
      .traverse(alg => Stream.chunk(Chunk.array(input)).covary[IO].through(Compressors.compress(alg)).compile.count)
      .map(sizes => forAll(sizes)(_ should be < (input.length / 10).toLong))
  }
