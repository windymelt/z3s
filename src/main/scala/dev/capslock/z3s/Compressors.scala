package dev.capslock.z3s

import cats.effect.IO
import com.github.luben.zstd.{ZstdInputStream, ZstdOutputStream}
import fs2.{Pipe, Stream}
import fs2.io.{readInputStream, readOutputStream, toInputStreamResource, writeOutputStream}
import java.io.{InputStream, OutputStream}
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

  private def viaOutputStream(wrap: OutputStream => OutputStream): Pipe[IO, Byte, Byte] = in =>
    readOutputStream[IO](ChunkSize) { os =>
      in.through(writeOutputStream(IO.blocking(wrap(os)), closeAfterUse = true)).compile.drain
    }

  private def viaInputStream(wrap: InputStream => InputStream): Pipe[IO, Byte, Byte] = in =>
    Stream.resource(toInputStreamResource(in)).flatMap { is =>
      readInputStream(IO.blocking(wrap(is)), ChunkSize, closeAfterUse = true)
    }
