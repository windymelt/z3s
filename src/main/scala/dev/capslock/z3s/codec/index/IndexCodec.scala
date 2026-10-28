package dev.capslock.z3s.codec.index

import dev.capslock.z3s.{Header, Index, IndexEntry}
import dev.capslock.z3s.codec.BorerSupport.{readArray, readMap, readU64, required, writeU64}
import dev.capslock.z3s.codec.header.HeaderCodec.given
import io.bullet.borer.{Cbor, Decoder, Encoder}

/** datasetごとのインデックスのCBOR表現。各エントリはヘッダをそのまま含む */
object IndexCodec:
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

  def encode(i: Index): Array[Byte] = Cbor.encode(i).toByteArray

  def decode(bytes: Array[Byte]): Either[String, Index] =
    Cbor.decode(bytes).to[Index].valueEither.left.map(e => s"invalid index: ${e.getMessage}")
