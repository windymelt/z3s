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

  /** 圧縮器へ1回に書く大きさ。手元の計測では、zstd:1は64KiBずつより1MiBずつ書くほうが約3倍速かった。
    * 4MiBにするとさらに速かったが、1本のストリームがこの数倍のメモリを使うので1MiBにとどめる
    */
  private val CompressChunkSize = 1024 * 1024

  def compress(c: Compression): Pipe[IO, Byte, Byte] = c match
    case Compression.None         => identity
    case Compression.Gzip(level)  => fs2.compression.Compression[IO].gzip(deflateLevel = Some(level))
    // 展開時に壊れたデータを検出できるよう、展開後の内容のチェックサムをフレームに付ける
    case Compression.Zstd(level)  => viaOutputStream(new ZstdOutputStream(_, level).setChecksum(true))
    case Compression.Xz(preset)   => viaOutputStream(new XZOutputStream(_, new LZMA2Options(preset)))
    case Compression.Bzip2(block) => viaOutputStream(new BZip2CompressorOutputStream(_, block))

  def decompress(c: Compression): Pipe[IO, Byte, Byte] = c match
    case Compression.None    => identity
    case Compression.Gzip(_) => _.through(fs2.compression.Compression[IO].gunzip(ChunkSize)).flatMap(_.content)
    case Compression.Zstd(_) => viaInputStream(new ZstdInputStream(_))
    case Compression.Xz(_)   => viaInputStream(new XZInputStream(_))
    case Compression.Bzip2(_) => viaInputStream(new BZip2CompressorInputStream(_))

  /** チャンクごとに圧縮器へ書き、メモリ上のバッファに出た圧縮データを流す。
    *
    * 圧縮器の出力先を下流へのパイプにすると、下流が読むのをやめてストリームが取り消されたとき、
    * 後始末のcloseが残りの圧縮データをパイプへ書こうとして、空きが出るのを待ち続ける。
    * バッファへの書き込みは読む側を待たないので、closeは下流の状態にかかわらず終わる
    */
  private def viaOutputStream(wrap: OutputStream => OutputStream): Pipe[IO, Byte, Byte] = in =>
    val open = IO.blocking {
      val buffer = new ByteArrayOutputStream()
      (buffer, wrap(buffer))
    }
    // 取り消しや失敗で終わった場合も、圧縮器が持つネイティブのメモリなどを解放する
    val close = (_: ByteArrayOutputStream, os: OutputStream) => IO.blocking(os.close()).attempt.void

    Stream.bracket(open)(close.tupled).flatMap { (buffer, os) =>
      /** 圧縮器がここまでにバッファへ書いたバイト列を取り出し、バッファを空にする */
      def drainBuffer(): Chunk[Byte] =
        val out = Chunk.array(buffer.toByteArray)
        buffer.reset()
        out

      // zfs sendの出力は8KiBずつ届く。圧縮器へ書く回数を減らすため、CompressChunkSizeまでまとめてから書く
      in.chunkMin(CompressChunkSize).evalMap(c => IO.blocking { os.write(c.toArray); drainBuffer() }).unchunks ++
        Stream.evalUnChunk(IO.blocking { os.close(); drainBuffer() })
    }

  /** 展開器はInputStreamから読むので、パイプを介する。取り消し時に読み出しのスレッドへ割り込めるよう、interruptibleで読む */
  private def viaInputStream(wrap: InputStream => InputStream): Pipe[IO, Byte, Byte] = in =>
    Stream.resource(toInputStreamResource(in)).flatMap { raw =>
      // 展開器には1バイトずつ読むものがある。fs2のInputStreamは読むたびにファイバーとやり取りするので、まとめて読む
      val open = IO.interruptible(wrap(new BufferedInputStream(raw, ChunkSize)))
      val close = (is: InputStream) => IO.blocking(is.close()).attempt.void

      Stream.bracket(open)(close).flatMap { is =>
        /** 展開したバイト列を最大ChunkSizeバイト読む。終わりに達したらNone */
        def readChunk(): Option[Chunk[Byte]] =
          val buf = new Array[Byte](ChunkSize)
          val n = is.read(buf)
          if n < 0 then None else Some(Chunk.array(buf, 0, n))

        Stream
          .repeatEval(IO.interruptible(readChunk()))
          .unNoneTerminate
          .unchunks
      }
    }
