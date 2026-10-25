package dev.capslock.z3s

import cats.effect.IO
import com.github.luben.zstd.{ZstdInputStream, ZstdOutputStream}
import fs2.{Chunk, Pipe, Stream}
import fs2.io.toInputStreamResource
import java.io.{BufferedInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import org.tukaani.xz.{LZMA2Options, XZInputStream, XZOutputStream}

/** すべての圧縮方式を`Pipe[IO, Byte, Byte]`として扱う */
object Compressors:
  private val ChunkSize = 64 * 1024

  def compress(c: Compression): Pipe[IO, Byte, Byte] = c match
    case Compression.None         => identity
    case Compression.Gzip(level)  => fs2.compression.Compression[IO].gzip(deflateLevel = Some(level))
    case Compression.Zstd(level)  => viaOutputStream(new ZstdOutputStream(_, level))
    case Compression.Xz(preset)   => viaOutputStream(new XZOutputStream(_, new LZMA2Options(preset)))
    case Compression.Bzip2(block) => viaOutputStream(new BZip2CompressorOutputStream(_, block))

  def decompress(c: Compression): Pipe[IO, Byte, Byte] = c match
    case Compression.None    => identity
    case Compression.Gzip(_) => _.through(fs2.compression.Compression[IO].gunzip(ChunkSize)).flatMap(_.content)
    case Compression.Zstd(_) => viaInputStream(new ZstdInputStream(_))
    case Compression.Xz(_)   => viaInputStream(new XZInputStream(_))
    case Compression.Bzip2(_) => viaInputStream(new BZip2CompressorInputStream(_))

  /** チャンクごとにメモリ上のバッファへ圧縮して出力する。読む側を待たないので、取り消し時のcloseでも止まらない */
  private def viaOutputStream(wrap: OutputStream => OutputStream): Pipe[IO, Byte, Byte] = in =>
    val open = IO.blocking {
      val buffer = new ByteArrayOutputStream()
      (buffer, wrap(buffer))
    }
    Stream.bracket(open)((_, os) => IO.blocking(os.close()).attempt.void).flatMap { (buffer, os) =>
      def take(): Chunk[Byte] =
        val out = Chunk.array(buffer.toByteArray)
        buffer.reset()
        out
      in.chunks.evalMap(c => IO.blocking { os.write(c.toArray); take() }).unchunks ++
        Stream.evalUnChunk(IO.blocking { os.close(); take() })
    }

  /** 展開器はInputStreamから読むので、パイプを介する。取り消し時に読み出しのスレッドへ割り込めるよう、interruptibleで読む */
  private def viaInputStream(wrap: InputStream => InputStream): Pipe[IO, Byte, Byte] = in =>
    Stream.resource(toInputStreamResource(in)).flatMap { raw =>
      // 展開器には1バイトずつ読むものがある。fs2のInputStreamは読むたびにファイバーとやり取りするので、まとめて読む
      Stream.bracket(IO.interruptible(wrap(new BufferedInputStream(raw, ChunkSize))))(is => IO.blocking(is.close()).attempt.void).flatMap { is =>
        Stream
          .repeatEval(IO.interruptible {
            val buf = new Array[Byte](ChunkSize)
            val n = is.read(buf)
            if n < 0 then None else Some(Chunk.array(buf, 0, n))
          })
          .unNoneTerminate
          .unchunks
      }
    }
