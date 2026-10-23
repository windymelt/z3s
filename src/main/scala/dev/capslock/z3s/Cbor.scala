package dev.capslock.z3s

import cats.data.NonEmptyList
import io.bullet.borer.{Cbor as BorerCbor, Decoder, Encoder, Reader, Writer}

/** ヘッダとインデックスのCBOR表現。mapは文字列キーとし、デコード時は未知のキーを読み飛ばす */
object Cbor:
  private[z3s] def fail(msg: String): Nothing = throw new IllegalArgumentException(msg)

  /** 符号なし64ビット値を`Long`のビット列のまま書く */
  private def writeU64(w: Writer, v: Long): Writer =
    if v >= 0 then w.writeLong(v) else w.writeOverLong(negative = false, v)

  private def readU64(r: Reader): Long =
    if r.hasOverLong then r.readOverLong()
    else
      val v = r.readLong()
      if v < 0 then fail(s"expected unsigned integer, got $v") else v

  /** mapの各要素を読み、`known`が扱わなかったキーの値を読み飛ばす */
  private[z3s] def readMap(r: Reader)(known: PartialFunction[String, Reader => Unit]): Unit =
    def field(): Unit =
      val key = r.readString()
      known.lift(key) match
        case Some(read) => read(r)
        case None       => r.skipElement()
    if r.hasMapHeader then
      val n = r.readMapHeader()
      var i = 0L
      while i < n do
        field()
        i += 1
    else
      r.readMapStart()
      while !r.tryReadBreak() do field()

  private[z3s] def readArray[A](r: Reader)(elem: Reader => A): List[A] =
    val b = List.newBuilder[A]
    if r.hasArrayHeader then
      val n = r.readArrayHeader()
      var i = 0L
      while i < n do
        b += elem(r)
        i += 1
    else
      r.readArrayStart()
      while !r.tryReadBreak() do b += elem(r)
    b.result()

  private[z3s] def required[A](name: String, v: Option[A]): A = v.getOrElse(fail(s"missing key: $name"))

  given Encoder[Compression] = Encoder { (w, c) =>
    val (alg, param) = c match
      case Compression.None         => ("none", scala.None)
      case Compression.Gzip(level)  => ("gzip", Some("level" -> level))
      case Compression.Zstd(level)  => ("zstd", Some("level" -> level))
      case Compression.Xz(preset)   => ("xz", Some("preset" -> preset))
      case Compression.Bzip2(block) => ("bzip2", Some("blockSize" -> block))
    param match
      case Some((k, v)) => w.writeMapHeader(2).writeString("alg").writeString(alg).writeString(k).writeInt(v)
      case scala.None   => w.writeMapHeader(1).writeString("alg").writeString(alg)
  }

  given Decoder[Compression] = Decoder { r =>
    var alg: Option[String] = scala.None
    var level: Option[Int] = scala.None
    var preset: Option[Int] = scala.None
    var block: Option[Int] = scala.None
    readMap(r) {
      case "alg"       => r => alg = Some(r.readString())
      case "level"     => r => level = Some(r.readInt())
      case "preset"    => r => preset = Some(r.readInt())
      case "blockSize" => r => block = Some(r.readInt())
    }
    required("alg", alg) match
      case "none"  => Compression.None
      case "gzip"  => Compression.Gzip(required("level", level))
      case "zstd"  => Compression.Zstd(required("level", level))
      case "xz"    => Compression.Xz(required("preset", preset))
      case "bzip2" => Compression.Bzip2(required("blockSize", block))
      case other   => fail(s"unknown compression: $other")
  }

  given Encoder[Encryption] = Encoder { (w, e) =>
    e match
      case Encryption.None => w.writeMapHeader(1).writeString("alg").writeString("none")
      case Encryption.Age(rs) =>
        w.writeMapHeader(2).writeString("alg").writeString("age").writeString("recipients")
        w.writeArrayHeader(rs.size)
        rs.toList.foreach(w.writeString)
        w
  }

  given Decoder[Encryption] = Decoder { r =>
    var alg: Option[String] = scala.None
    var recipients: List[String] = Nil
    readMap(r) {
      case "alg"        => r => alg = Some(r.readString())
      case "recipients" => r => recipients = readArray(r)(_.readString())
    }
    required("alg", alg) match
      case "none" => Encryption.None
      case "age" =>
        NonEmptyList.fromList(recipients).map(Encryption.Age(_)).getOrElse(fail("age without recipients"))
      case other => fail(s"unknown encryption: $other")
  }

  given Encoder[SendMode] = Encoder { (w, m) =>
    m match
      case SendMode.Raw => w.writeMapHeader(1).writeString("kind").writeString("raw")
      case SendMode.Plain(c, e) =>
        w.writeMapHeader(3)
          .writeString("kind").writeString("plain")
          .writeString("compression").write(c)
          .writeString("encryption").write(e)
  }

  given Decoder[SendMode] = Decoder { r =>
    var kind: Option[String] = scala.None
    var compression: Option[Compression] = scala.None
    var encryption: Option[Encryption] = scala.None
    readMap(r) {
      case "kind"        => r => kind = Some(r.readString())
      case "compression" => r => compression = Some(r.read[Compression]())
      case "encryption"  => r => encryption = Some(r.read[Encryption]())
    }
    required("kind", kind) match
      case "raw"   => SendMode.Raw
      case "plain" => SendMode.Plain(required("compression", compression), required("encryption", encryption))
      case other   => fail(s"unknown mode: $other")
  }

  given Encoder[ParentRef] = Encoder { (w, p) =>
    w.writeMapHeader(2).writeString("name").writeString(p.name).writeString("guid")
    writeU64(w, p.guid)
  }

  given Decoder[ParentRef] = Decoder { r =>
    var name: Option[String] = scala.None
    var guid: Option[Long] = scala.None
    readMap(r) {
      case "name" => r => name = Some(r.readString())
      case "guid" => r => guid = Some(readU64(r))
    }
    ParentRef(required("name", name), required("guid", guid))
  }

  given Encoder[Header] = Encoder { (w, h) =>
    w.writeMapHeader(if h.parent.isDefined then 8 else 7)
    w.writeString("snapshot").writeString(h.snapshot)
    w.writeString("guid"); writeU64(w, h.guid)
    w.writeString("createtxg"); writeU64(w, h.createtxg)
    h.parent.foreach(p => w.writeString("parent").write(p))
    w.writeString("mode").write(h.mode)
    w.writeString("estimatedSize"); writeU64(w, h.estimatedSize)
    w.writeString("createdAt").writeString(h.createdAt)
    w.writeString("toolVersion").writeString(h.toolVersion)
  }

  given Decoder[Header] = Decoder { r =>
    var snapshot: Option[String] = scala.None
    var guid: Option[Long] = scala.None
    var createtxg: Option[Long] = scala.None
    var parent: Option[ParentRef] = scala.None
    var mode: Option[SendMode] = scala.None
    var estimatedSize: Option[Long] = scala.None
    var createdAt: Option[String] = scala.None
    var toolVersion: Option[String] = scala.None
    readMap(r) {
      case "snapshot"      => r => snapshot = Some(r.readString())
      case "guid"          => r => guid = Some(readU64(r))
      case "createtxg"     => r => createtxg = Some(readU64(r))
      case "parent"        => r => parent = if r.tryReadNull() then scala.None else Some(r.read[ParentRef]())
      case "mode"          => r => mode = Some(r.read[SendMode]())
      case "estimatedSize" => r => estimatedSize = Some(readU64(r))
      case "createdAt"     => r => createdAt = Some(r.readString())
      case "toolVersion"   => r => toolVersion = Some(r.readString())
    }
    Header(
      required("snapshot", snapshot),
      required("guid", guid),
      required("createtxg", createtxg),
      parent,
      required("mode", mode),
      required("estimatedSize", estimatedSize),
      required("createdAt", createdAt),
      required("toolVersion", toolVersion)
    )
  }

  given Encoder[IndexEntry] = Encoder { (w, e) =>
    w.writeMapHeader(5)
    w.writeString("key").writeString(e.key)
    w.writeString("header").write(e.header)
    w.writeString("size"); writeU64(w, e.size)
    w.writeString("etag").writeString(e.etag)
    w.writeString("uploadedAt").writeString(e.uploadedAt)
  }

  given Decoder[IndexEntry] = Decoder { r =>
    var key: Option[String] = scala.None
    var header: Option[Header] = scala.None
    var size: Option[Long] = scala.None
    var etag: Option[String] = scala.None
    var uploadedAt: Option[String] = scala.None
    readMap(r) {
      case "key"        => r => key = Some(r.readString())
      case "header"     => r => header = Some(r.read[Header]())
      case "size"       => r => size = Some(readU64(r))
      case "etag"       => r => etag = Some(r.readString())
      case "uploadedAt" => r => uploadedAt = Some(r.readString())
    }
    IndexEntry(
      required("key", key),
      required("header", header),
      required("size", size),
      required("etag", etag),
      required("uploadedAt", uploadedAt)
    )
  }

  given Encoder[Index] = Encoder { (w, i) =>
    w.writeMapHeader(2)
    w.writeString("schemaVersion").writeInt(i.schemaVersion)
    w.writeString("entries").writeArrayHeader(i.entries.size)
    i.entries.foreach(e => w.write(e))
    w
  }

  given Decoder[Index] = Decoder { r =>
    var schemaVersion: Option[Int] = scala.None
    var entries: List[IndexEntry] = Nil
    readMap(r) {
      case "schemaVersion" => r => schemaVersion = Some(r.readInt())
      case "entries"       => r => entries = readArray(r)(_.read[IndexEntry]())
    }
    Index(required("schemaVersion", schemaVersion), entries)
  }

  def encodeHeader(h: Header): Array[Byte] = BorerCbor.encode(h).toByteArray

  def decodeHeader(bytes: Array[Byte]): Either[String, Header] =
    BorerCbor.decode(bytes).to[Header].valueEither.left.map(e => s"invalid header: ${e.getMessage}")

  def encodeIndex(i: Index): Array[Byte] = BorerCbor.encode(i).toByteArray

  def decodeIndex(bytes: Array[Byte]): Either[String, Index] =
    BorerCbor.decode(bytes).to[Index].valueEither.left.map(e => s"invalid index: ${e.getMessage}")
