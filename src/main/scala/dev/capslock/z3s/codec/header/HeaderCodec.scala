package dev.capslock.z3s.codec.header

import cats.data.NonEmptyList
import dev.capslock.z3s.{ Compression, Encryption, Header, ParentRef, PropertyValue, SendMode, ZfsVersion }
import dev.capslock.z3s.codec.BorerSupport.{ fail, readArray, readMap, readU64, required, writeU64 }
import io.bullet.borer.{ Cbor, Decoder, Encoder, Reader, Writer }


/** バックアップのオブジェクトのヘッダのCBOR表現。mapは文字列キーとし、デコード時は未知のキーを読み飛ばす */
object HeaderCodec:
  given Encoder[Compression] = Encoder: (w, c) =>
    val (alg, param) = c match
      case Compression.None         => ("none", scala.None)
      case Compression.Gzip(level)  => ("gzip", Some("level" -> level))
      case Compression.Zstd(level)  => ("zstd", Some("level" -> level))
      case Compression.Xz(preset)   => ("xz", Some("preset" -> preset))
      case Compression.Bzip2(block) => ("bzip2", Some("blockSize" -> block))
    param match
      case Some((k, v)) => w
        .writeMapHeader(2)
        .writeString("alg")
        .writeString(alg)
        .writeString(k)
        .writeInt(v)
      case scala.None => w.writeMapHeader(1).writeString("alg").writeString(alg)

  given Decoder[Compression] = Decoder: r =>
    var alg: Option[String] = scala.None
    var level: Option[Int] = scala.None
    var preset: Option[Int] = scala.None
    var block: Option[Int] = scala.None
    readMap(r):
      case "alg"       => r => alg = Some(r.readString())
      case "level"     => r => level = Some(r.readInt())
      case "preset"    => r => preset = Some(r.readInt())
      case "blockSize" => r => block = Some(r.readInt())
    required("alg", alg) match
      case "none"  => Compression.None
      case "gzip"  => Compression.Gzip(required("level", level))
      case "zstd"  => Compression.Zstd(required("level", level))
      case "xz"    => Compression.Xz(required("preset", preset))
      case "bzip2" => Compression.Bzip2(required("blockSize", block))
      case other   => fail(s"unknown compression: $other")

  given Encoder[Encryption] = Encoder: (w, e) =>
    e match
      case Encryption.None    => w.writeMapHeader(1).writeString("alg").writeString("none")
      case Encryption.Age(rs) =>
        w.writeMapHeader(2).writeString("alg").writeString("age").writeString("recipients")
        w.writeArrayHeader(rs.size)
        rs.toList.foreach(w.writeString)
        w

  given Decoder[Encryption] = Decoder: r =>
    var alg: Option[String] = scala.None
    var recipients: List[String] = Nil
    readMap(r):
      case "alg"        => r => alg = Some(r.readString())
      case "recipients" => r => recipients = readArray(r)(_.readString())
    required("alg", alg) match
      case "none" => Encryption.None
      case "age"  =>
        NonEmptyList
          .fromList(recipients)
          .map(Encryption.Age(_))
          .getOrElse(fail("age without recipients"))
      case other => fail(s"unknown encryption: $other")

  given Encoder[SendMode] = Encoder: (w, m) =>
    m match
      case SendMode.Raw         => w.writeMapHeader(1).writeString("kind").writeString("raw")
      case SendMode.Plain(c, e) =>
        w
          .writeMapHeader(3)
          .writeString("kind")
          .writeString("plain")
          .writeString("compression")
          .write(c)
          .writeString("encryption")
          .write(e)

  given Decoder[SendMode] = Decoder: r =>
    var kind: Option[String] = scala.None
    var compression: Option[Compression] = scala.None
    var encryption: Option[Encryption] = scala.None
    readMap(r):
      case "kind"        => r => kind = Some(r.readString())
      case "compression" => r => compression = Some(r.read[Compression]())
      case "encryption"  => r => encryption = Some(r.read[Encryption]())
    required("kind", kind) match
      case "raw"   => SendMode.Raw
      case "plain" => SendMode.Plain(
        required("compression", compression),
        required("encryption", encryption),
      )
      case other => fail(s"unknown mode: $other")

  given Encoder[ParentRef] = Encoder: (w, p) =>
    w.writeMapHeader(2).writeString("name").writeString(p.name).writeString("guid")
    writeU64(w, p.guid)

  given Decoder[ParentRef] = Decoder: r =>
    var name: Option[String] = scala.None
    var guid: Option[Long] = scala.None
    readMap(r):
      case "name" => r => name = Some(r.readString())
      case "guid" => r => guid = Some(readU64(r))
    ParentRef(required("name", name), required("guid", guid))

  given Encoder[ZfsVersion] = Encoder: (w, v) =>
    w.writeMapHeader(1 + v.kernel.size).writeString("userland").writeString(v.userland)
    v.kernel.foreach(k => w.writeString("kernel").writeString(k))
    w

  given Decoder[ZfsVersion] = Decoder: r =>
    var userland: Option[String] = scala.None
    var kernel: Option[String] = scala.None
    readMap(r):
      case "userland" => r => userland = Some(r.readString())
      case "kernel"   => r => kernel = Some(r.readString())
    ZfsVersion(required("userland", userland), kernel)

  given Encoder[PropertyValue] = Encoder: (w, p) =>
    w
      .writeMapHeader(2)
      .writeString("value")
      .writeString(p.value)
      .writeString("source")
      .writeString(p.source)

  given Decoder[PropertyValue] =
    Decoder: r =>
      var value: Option[String] = scala.None
      var source: Option[String] = scala.None
      readMap(r):
        case "value"  => r => value = Some(r.readString())
        case "source" => r => source = Some(r.readString())
      PropertyValue(required("value", value), required("source", source))

  /** 名前の順に書き、同じプロパティから同じバイト列を作る */
  private def writeProperties(w: Writer, ps: Map[String, PropertyValue]): Writer =
    w.writeMapHeader(ps.size)
    ps.toList.sortBy(_._1).foreach((k, v) => w.writeString(k).write(v))
    w

  private def readProperties(r: Reader): Map[String, PropertyValue] =
    val b = Map.newBuilder[String, PropertyValue]
    readMap(r):
      case k => r => b += k -> r.read[PropertyValue]()
    b.result()

  given Encoder[Header] = Encoder: (w, h) =>
    w.writeMapHeader(
      7 + h.parent.size + h.partSize.size + h.zfsVersion.size + h.properties.size + h.snapshotProperties.size
    )
    w.writeString("snapshot").writeString(h.snapshot)
    w.writeString("guid"); writeU64(w, h.guid)
    w.writeString("createtxg"); writeU64(w, h.createtxg)
    h.parent.foreach(p => w.writeString("parent").write(p))
    w.writeString("mode").write(h.mode)
    w.writeString("estimatedSize"); writeU64(w, h.estimatedSize)
    w.writeString("createdAt").writeString(h.createdAt)
    w.writeString("toolVersion").writeString(h.toolVersion)
    h.partSize.foreach(n => writeU64(w.writeString("partSize"), n))
    h.zfsVersion.foreach(v => w.writeString("zfsVersion").write(v))
    h.properties.foreach(ps => writeProperties(w.writeString("properties"), ps))
    h.snapshotProperties.foreach(ps => writeProperties(w.writeString("snapshotProperties"), ps))
    w

  given Decoder[Header] = Decoder: r =>
    var snapshot: Option[String] = scala.None
    var guid: Option[Long] = scala.None
    var createtxg: Option[Long] = scala.None
    var parent: Option[ParentRef] = scala.None
    var mode: Option[SendMode] = scala.None
    var estimatedSize: Option[Long] = scala.None
    var createdAt: Option[String] = scala.None
    var toolVersion: Option[String] = scala.None
    var partSize: Option[Long] = scala.None
    var zfsVersion: Option[ZfsVersion] = scala.None
    var properties: Option[Map[String, PropertyValue]] = scala.None
    var snapshotProperties: Option[Map[String, PropertyValue]] = scala.None
    readMap(r):
      case "snapshot"  => r => snapshot = Some(r.readString())
      case "guid"      => r => guid = Some(readU64(r))
      case "createtxg" => r => createtxg = Some(readU64(r))
      case "parent"    => r => parent = if r.tryReadNull() then scala.None else Some(
        r.read[ParentRef]()
      )
      case "mode"               => r => mode = Some(r.read[SendMode]())
      case "estimatedSize"      => r => estimatedSize = Some(readU64(r))
      case "createdAt"          => r => createdAt = Some(r.readString())
      case "toolVersion"        => r => toolVersion = Some(r.readString())
      case "partSize"           => r => partSize = Some(readU64(r))
      case "zfsVersion"         => r => zfsVersion = Some(r.read[ZfsVersion]())
      case "properties"         => r => properties = Some(readProperties(r))
      case "snapshotProperties" => r => snapshotProperties = Some(readProperties(r))
    Header(
      required("snapshot", snapshot),
      required("guid", guid),
      required("createtxg", createtxg),
      parent,
      required("mode", mode),
      required("estimatedSize", estimatedSize),
      required("createdAt", createdAt),
      required("toolVersion", toolVersion),
      partSize,
      zfsVersion,
      properties,
      snapshotProperties,
    )

  def encode(h: Header): Array[Byte] = Cbor.encode(h).toByteArray

  def decode(bytes: Array[Byte]): Either[String, Header] = Cbor
    .decode(bytes)
    .to[Header]
    .valueEither
    .left
    .map(e => s"invalid header: ${e.getMessage}")
end HeaderCodec
