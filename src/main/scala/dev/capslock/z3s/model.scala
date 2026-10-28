package dev.capslock.z3s

import cats.data.NonEmptyList
import scodec.bits.ByteVector

enum SendMode:
  case Raw
  case Plain(compression: Compression, encryption: Encryption)

  def kind: ModeKind = this match
    case Raw         => ModeKind.Raw
    case Plain(_, _) => ModeKind.Plain

enum ModeKind:
  case Raw, Plain

  def label: String = this match
    case Raw   => "raw"
    case Plain => "plain"

enum Compression:
  case None
  case Gzip(level: Int)
  case Zstd(level: Int)
  case Xz(preset: Int)
  case Bzip2(blockSize: Int)

  def label: String = this match
    case None         => "none"
    case Gzip(level)  => s"gzip:$level"
    case Zstd(level)  => s"zstd:$level"
    case Xz(preset)   => s"xz:$preset"
    case Bzip2(block) => s"bzip2:$block"

object Compression:
  val Default: Compression = Zstd(3)

  /** `--compress`の値(`ALG[:LEVEL]`)を解釈する */
  def parse(s: String): Either[String, Compression] =
    def level(alg: String, param: Option[String], min: Int, max: Int, default: Int): Either[String, Int] =
      param match
        case scala.None => Right(default)
        case Some(p) =>
          p.toIntOption
            .filter(n => n >= min && n <= max)
            .toRight(s"$alg: level must be an integer in $min-$max: $p")

    s.split(":", 2) match
      case Array(alg, rest*) =>
        val param = rest.headOption
        alg match
          case "none" if param.isEmpty => Right(None)
          case "gzip"                  => level(alg, param, 1, 9, 6).map(Gzip(_))
          case "zstd"                  => level(alg, param, 1, 19, 3).map(Zstd(_))
          case "xz"                    => level(alg, param, 0, 9, 6).map(Xz(_))
          case "bzip2"                 => level(alg, param, 1, 9, 9).map(Bzip2(_))
          case _                       => Left(s"unknown compression: $s")
      case _ => Left(s"unknown compression: $s")

enum Encryption:
  case None
  case Age(recipients: NonEmptyList[String])

  def label: String = this match
    case None    => "none"
    case Age(rs) => s"age(${rs.size} recipients)"

final case class Snapshot(name: String, guid: Long, createtxg: Long):
  /** `@`より後ろの部分 */
  def shortName: String = name.substring(name.indexOf('@') + 1)
  def dataset: String = name.substring(0, name.indexOf('@'))

final case class ParentRef(name: String, guid: Long)

final case class Header(
    snapshot: String,
    guid: Long,
    createtxg: Long,
    parent: Option[ParentRef],
    mode: SendMode,
    estimatedSize: Long,
    createdAt: String,
    toolVersion: String,
    /** multipart uploadのパートの大きさ。古いバージョンが書いたヘッダにはない */
    partSize: Option[Long] = None
):
  def isFull: Boolean = parent.isEmpty

final case class Md5(bytes: ByteVector) extends AnyVal:
  def hex: String = bytes.toHex

object Md5:
  /** 16バイトを表す16進の文字列でなければNoneを返す */
  def fromHex(hex: String): Option[Md5] = ByteVector.fromHex(hex).filter(_.size == 16).map(Md5(_))

/** バックアップのオブジェクトの各パートのMD5。多くのS3互換ストレージでは、ETagをこれらから計算して照合できる */
final case class PartDigests(partSize: Long, size: Long, partMd5s: List[Md5])

final case class IndexEntry(
    key: String,
    header: Header,
    size: Long,
    etag: String,
    uploadedAt: String
)

final case class Index(schemaVersion: Int, entries: List[IndexEntry]):
  /** 同じキーのentryを置き換えて追加する */
  def upsert(entry: IndexEntry): Index =
    copy(entries = entries.filterNot(_.key == entry.key) :+ entry)

object Index:
  val SchemaVersion = 1
  val empty: Index = Index(SchemaVersion, Nil)

enum Health:
  case Ok, MissingParent, ParentBroken, ModeMismatch, Cycle

  def label: String = this match
    case Ok            => "ok"
    case MissingParent => "missing parent"
    case ParentBroken  => "parent broken"
    case ModeMismatch  => "mode mismatch"
    case Cycle         => "cycle"

/** guidは符号なし64ビットとして表示する */
def showGuid(guid: Long): String = java.lang.Long.toUnsignedString(guid)
