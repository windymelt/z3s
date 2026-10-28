package dev.capslock.z3s

import cats.effect.IO
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import scala.concurrent.duration.*
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

  def decompressAll(alg: Compression, compressed: Array[Byte]): IO[Array[Byte]] =
    Stream.chunk(Chunk.array(compressed)).covary[IO].through(Compressors.decompress(alg)).compile.to(Array)

  test("zstd detects a corrupted payload") {
    // 乱数の部分は圧縮されずにそのまま格納されるので、1バイト変えても展開自体は通る
    Stream.chunk(Chunk.array(data)).covary[IO].through(Compressors.compress(Compression.Zstd(3))).compile.to(Array).flatMap {
      compressed =>
        compressed(1000) = (compressed(1000) ^ 1).toByte
        decompressAll(Compression.Zstd(3), compressed).attempt.map(_.isLeft shouldBe true)
    }
  }

  test("zstd decompresses frames without a checksum") {
    val buffer = new java.io.ByteArrayOutputStream()
    val zstd = new com.github.luben.zstd.ZstdOutputStream(buffer, 3)
    zstd.write(data)
    zstd.close()
    decompressAll(Compression.Zstd(3), buffer.toByteArray).map(out => ByteVector(out) should === (ByteVector(data)))
  }

  /** 圧縮しにくいデータを終わりなく流す */
  val endless: Stream[IO, Byte] =
    val rnd = new scala.util.Random(1)
    Stream.repeatEval(IO(Chunk.array(Array.fill(64 * 1024)(rnd.nextInt(256).toByte)))).unchunks

  /** ioが失敗や取り消しで終わった場合は失敗にする。取り消しが終わらない場合も、ioの完了を待つのを打ち切って失敗にする。
    * ioのファイバーは取り消さずに残す
    */
  def completesWithin(limit: FiniteDuration)(io: IO[Unit]) =
    io.start
      .flatMap(_.joinWith(IO.raiseError(new AssertionError("canceled"))).as(true).timeoutTo(limit, IO.pure(false)))
      .map(completed => withClue(s"did not complete within $limit: ")(completed shouldBe true))

  for alg <- algorithms.filter(_ != Compression.None) do
    test(s"compression stops when the downstream stops reading: ${alg.label}") {
      // 下流が読むのをやめると上流は取り消される。圧縮器の終了処理が出力を書けずに待つと、ここで止まる
      completesWithin(5.seconds)(endless.through(Compressors.compress(alg)).take(1).compile.drain)
    }

    test(s"decompression stops when the downstream stops reading: ${alg.label}") {
      endless.take(8L * 1024 * 1024).through(Compressors.compress(alg)).compile.to(Array).flatMap { compressed =>
        completesWithin(5.seconds)(
          Stream.chunk(Chunk.array(compressed)).covary[IO].through(Compressors.decompress(alg)).take(1).compile.drain
        )
      }
    }
