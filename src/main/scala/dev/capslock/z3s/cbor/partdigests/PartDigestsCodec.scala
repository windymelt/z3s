package dev.capslock.z3s.cbor.partdigests

import dev.capslock.z3s.{Md5, PartDigests}
import dev.capslock.z3s.Cbor.{fail, readArray, readMap, readU64, required, writeU64}
import io.bullet.borer.{Cbor as BorerCbor, Decoder, Encoder}
import scodec.bits.ByteVector

/** パートのMD5一覧のCBOR表現。ヘッダやインデックスとは別のオブジェクトに保存するので、版も別に持つ */
object PartDigestsCodec:
  private val SchemaVersion = 1
  private val Md5Length = 16

  given Encoder[PartDigests] = Encoder { (w, d) =>
    w.writeMapHeader(4)
    w.writeString("schemaVersion").writeInt(SchemaVersion)
    w.writeString("partSize"); writeU64(w, d.partSize)
    w.writeString("size"); writeU64(w, d.size)
    w.writeString("md5").writeArrayHeader(d.partMd5s.size)
    d.partMd5s.foreach(m => w.writeBytes(m.bytes.toArray))
    w
  }

  given Decoder[PartDigests] = Decoder { r =>
    var schemaVersion: Option[Int] = None
    var partSize: Option[Long] = None
    var size: Option[Long] = None
    var md5s: Option[List[Md5]] = None
    readMap(r) {
      case "schemaVersion" => r => schemaVersion = Some(r.readInt())
      case "partSize"      => r => partSize = Some(readU64(r))
      case "size"          => r => size = Some(readU64(r))
      case "md5" =>
        r =>
          md5s = Some(readArray(r) { r =>
            val b = r.readByteArray()
            if b.length != Md5Length then fail(s"MD5 must be $Md5Length bytes, got ${b.length}") else Md5(ByteVector(b))
          })
    }
    required("schemaVersion", schemaVersion) match
      case SchemaVersion => PartDigests(required("partSize", partSize), required("size", size), required("md5", md5s))
      case other         => fail(s"unsupported part digests schema version $other")
  }

  def encode(d: PartDigests): Array[Byte] = BorerCbor.encode(d).toByteArray

  def decode(bytes: Array[Byte]): Either[String, PartDigests] =
    BorerCbor.decode(bytes).to[PartDigests].valueEither.left.map(e => s"invalid part digests: ${e.getMessage}")
