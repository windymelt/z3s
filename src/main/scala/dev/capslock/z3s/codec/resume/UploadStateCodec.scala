package dev.capslock.z3s.codec.resume

import dev.capslock.z3s.{Md5, PartRecord, UploadState}
import dev.capslock.z3s.codec.BorerSupport.{fail, readArray, readMap, required}
import io.bullet.borer.{Decoder, Encoder, Json}
import scodec.bits.ByteVector

/** 再開状態のファイルのJSON表現 */
object UploadStateCodec:
  private val SchemaVersion = 1

  given Encoder[UploadState] = Encoder { (w, s) =>
    // borerのJSON出力は長さ付きのarrayとmapに対応しないため、不定長で書く
    w.writeMapStart()
    w.writeString("schemaVersion").writeInt(SchemaVersion)
    w.writeString("bucket").writeString(s.bucket)
    w.writeString("key").writeString(s.key)
    w.writeString("uploadId").writeString(s.uploadId)
    w.writeString("partSize").writeInt(s.partSize)
    w.writeString("header").writeString(s.header.toBase64)
    w.writeString("parts").writeArrayStart()
    s.parts.foreach { p =>
      w.writeMapStart()
      w.writeString("number").writeInt(p.number)
      w.writeString("md5").writeString(p.md5.hex)
      w.writeString("size").writeLong(p.size)
      w.writeBreak()
    }
    w.writeBreak()
    w.writeBreak()
  }

  given Decoder[PartRecord] = Decoder { r =>
    var number: Option[Int] = None
    var md5: Option[Md5] = None
    var size: Option[Long] = None
    readMap(r) {
      case "number" => r => number = Some(r.readInt())
      case "md5" =>
        r =>
          val hex = r.readString()
          md5 = Some(Md5.fromHex(hex).getOrElse(fail(s"invalid MD5: $hex")))
      case "size"   => r => size = Some(r.readLong())
    }
    PartRecord(required("number", number), required("md5", md5), required("size", size))
  }

  given Decoder[UploadState] = Decoder { r =>
    var version: Option[Int] = None
    var bucket: Option[String] = None
    var key: Option[String] = None
    var uploadId: Option[String] = None
    var partSize: Option[Int] = None
    var header: Option[ByteVector] = None
    var parts: List[PartRecord] = Nil
    readMap(r) {
      case "schemaVersion" => r => version = Some(r.readInt())
      case "bucket"        => r => bucket = Some(r.readString())
      case "key"           => r => key = Some(r.readString())
      case "uploadId"      => r => uploadId = Some(r.readString())
      case "partSize"      => r => partSize = Some(r.readInt())
      case "header" =>
        r => header = Some(ByteVector.fromBase64(r.readString()).getOrElse(fail("invalid header")))
      case "parts" => r => parts = readArray(r)(_.read[PartRecord]())
    }
    if !version.contains(SchemaVersion) then fail(s"unsupported schemaVersion: $version")
    UploadState(
      required("bucket", bucket),
      required("key", key),
      required("uploadId", uploadId),
      required("partSize", partSize),
      required("header", header),
      parts
    )
  }

  def encode(s: UploadState): Array[Byte] = Json.encode(s).toByteArray

  def decode(bytes: Array[Byte]): Either[String, UploadState] =
    Json.decode(bytes).to[UploadState].valueEither.left.map(_.getMessage)
