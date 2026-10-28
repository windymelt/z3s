package dev.capslock.z3s

import cats.effect.IO
import cats.syntax.all.*
import fs2.{Pipe, Pull, Stream}
import java.security.MessageDigest
import scodec.bits.ByteVector

/** multipart uploadのETag。多くのS3互換ストレージは、各パートのMD5を連結したもののMD5とパートの数からETagを作る */
final case class MultipartEtag(md5: Md5, parts: Int):
  def show: String = s"${md5.hex}-$parts"

object MultipartEtag:
  private val Form = """"?([0-9a-fA-F]{32})-([0-9]+)"?""".r

  /** `<MD5>-<パートの数>`の形でなければNoneを返す */
  def parse(etag: String): Option[MultipartEtag] = etag match
    case Form(hex, n) => (Md5.fromHex(hex.toLowerCase), n.toIntOption).mapN(MultipartEtag(_, _))
    case _            => None

  def of(partMd5s: List[Md5]): MultipartEtag =
    val md = MessageDigest.getInstance("MD5")
    partMd5s.foreach(m => md.update(m.bytes.toArray))
    MultipartEtag(Md5(ByteVector(md.digest())), partMd5s.size)

  /** sizeバイトのオブジェクトをpartSizeごとに区切ったときのパートの数 */
  def partCount(size: Long, partSize: Long): Long = (size + partSize - 1) / partSize

  /** 入力をpartSizeバイトずつに区切り、各パートのMD5を順に返す。パートを丸ごとメモリに置かない
    *
    * 使うメモリは、MessageDigestの状態(数百バイト)と、上流から受け取った処理中のチャンク1つ分で、partSizeには依存しない。
    * 結果をListで受け取る場合は、別に1パートあたり200バイト程度(16バイトのMD5と、ByteVectorやListのオブジェクト)がかかる。
    * パートは最大10,000個なので、2MB程度になる
    */
  def partMd5s(partSize: Long): Pipe[IO, Byte, Md5] = in =>
    /** mdに入れたバイト列のMD5を1つ出力する。MessageDigest.digestはmdを初期状態に戻すので、mdを次のパートに使える */
    def digest(md: MessageDigest): Pull[IO, Md5, Unit] = Pull.eval(IO(Md5(ByteVector(md.digest())))).flatMap(Pull.output1)

    /** sを読んでmdに入れ、パートが埋まるたびにそのMD5を出力する。sが尽きたときに途中までのパートがあれば、そのMD5も出力する
      *
      * @param filled 処理中のパートにすでに入れたバイト数
      */
    def go(s: Stream[IO, Byte], md: MessageDigest, filled: Long): Pull[IO, Md5, Unit] =
      s.pull.uncons.flatMap {
        case None => if filled > 0 then digest(md) else Pull.done
        case Some((chunk, rest)) =>
          val n = math.min(chunk.size.toLong, partSize - filled).toInt
          val slice = chunk.take(n).toArraySlice
          Pull.eval(IO(md.update(slice.values, slice.offset, slice.length))) >> {
            if filled + n < partSize then go(rest, md, filled + n)
            else digest(md) >> go(rest.cons(chunk.drop(n)), md, 0)
          }
      }
    Stream.eval(IO(MessageDigest.getInstance("MD5"))).flatMap(md => go(in, md, 0).stream)
