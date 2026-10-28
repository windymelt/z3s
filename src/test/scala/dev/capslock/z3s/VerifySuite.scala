package dev.capslock.z3s

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import dev.capslock.z3s.ops.{BackupOptions, Ops, VerifyOptions}
import fs2.{Chunk, Stream}
import java.security.MessageDigest
import scodec.bits.ByteVector

class MultipartEtagSuite extends IOSpec:
  def md5(bytes: Array[Byte]): Md5 = Md5(ByteVector(MessageDigest.getInstance("MD5").digest(bytes)))

  test("parse accepts <MD5>-<parts> with or without quotes and rejects other forms") {
    val hex = "0123456789abcdef0123456789ABCDEF"
    MultipartEtag.parse(s"\"$hex-12\"").value should === (MultipartEtag(Md5.fromHex(hex.toLowerCase).get, 12))
    MultipartEtag.parse(s"$hex-1").map(_.parts) should === (Some(1))
    MultipartEtag.parse(s"\"${hex.toLowerCase}\"") should === (None)
    MultipartEtag.parse(s"${hex.take(31)}-2") should === (None)
    MultipartEtag.parse(s"$hex-99999999999") should === (None)
    IO.unit.asserting(_ => succeed)
  }

  test("partMd5s splits the input at part boundaries regardless of chunking") {
    val bytes = Array.tabulate[Byte](1000)(i => (i * 31).toByte)
    val cases = List(1000L -> 1, 300L -> 4, 250L -> 4, 1L -> 1000, 2000L -> 1)
    cases.traverse { (partSize, parts) =>
      Stream
        .chunk(Chunk.array(bytes))
        .rechunkRandomly(0.01, 3.0)
        .through(MultipartEtag.partMd5s(partSize))
        .compile
        .toList
        .map(actual => (actual, bytes.grouped(partSize.toInt).map(md5).toList, parts))
    }.asserting { results =>
      forAll(results) { (actual, expected, parts) =>
        actual should === (expected)
        actual.size should === (parts)
      }
    }
  }

  test("partMd5s of an empty input has no parts") {
    Stream.empty.covary[IO].through(MultipartEtag.partMd5s(10)).compile.toList.asserting(_ should === (Nil))
  }

  test("of computes the MD5 of the concatenated part MD5s") {
    val parts = List(Array[Byte](1, 2, 3), Array[Byte](4, 5)).map(md5)
    IO(MultipartEtag.of(parts)).asserting { etag =>
      etag.md5 should === (md5(parts.flatMap(_.bytes.toArray).toArray))
      etag.parts should === (2)
    }
  }

/** zfsとS3をフェイクに置き換え、backupしたオブジェクトをverifyする */
class VerifySuite extends IOSpec:
  val dataset = "tank/data"
  val snapshots = List(Snapshot(s"$dataset@daily-1", 101, 10), Snapshot(s"$dataset@daily-2", 102, 20))
  val objectKey = s"backups/$dataset@daily-1"
  val partsKey = s"backups/$dataset/.z3s-parts/daily-1.cbor"
  val partSize: Int = Planner.MinPartSize.toInt

  final case class Env(store: InMemoryStore, ops: Ops)

  /** 3パートになる大きさのdaily-1をrawで送る */
  def env: IO[Env] =
    for
      ds <- Ref.of[IO, Map[String, List[Snapshot]]](Map(dataset -> snapshots))
      store = InMemoryStore()
      common = Common("bucket", "backups/", dataset, None, dryRun = false, concurrency = 2)
      ops = Ops(common, FakeZfs(ds, 2 * partSize + 1000), store)
      _ <- ops.backup(BackupOptions(full = true, Some("daily-1"), SendMode.Raw, None))
    yield Env(store, ops)

  def verify(e: Env, snapshot: Option[String] = None): IO[Either[Throwable, Unit]] =
    e.ops.verify(VerifyOptions(snapshot)).attempt

  def update(e: Env, k: String)(f: StoredObject => StoredObject): IO[Unit] =
    e.store.objects.update(m => m.updated(k, f(m(k))))

  /** 2番目のパートの1バイトを書き換える */
  def corruptPart2(e: Env): IO[Unit] =
    update(e, objectKey)(o => o.copy(bytes = o.bytes.updated(partSize + 10, (o.bytes(partSize + 10) ^ 1).toByte)))

  def failure(r: Either[Throwable, Unit]): String = r.left.value.getMessage

  test("an intact object passes the verification") {
    for
      e <- env
      etag <- e.store.head(objectKey).map(_.value.etag)
      r <- verify(e)
    yield
      MultipartEtag.parse(etag).map(_.parts) should === (Some(3))
      r should === (Right(()))
  }

  test("a damaged part is located with the part MD5 list") {
    for
      e <- env
      _ <- corruptPart2(e)
      r <- verify(e)
    yield failure(r) should include(s"$objectKey (corrupt part(s) 2)")
  }

  test("without the part MD5 list, damage is still detected but not located") {
    for
      e <- env
      _ <- e.store.objects.update(_ - partsKey)
      intact <- verify(e)
      _ <- corruptPart2(e)
      damaged <- verify(e)
    yield
      intact should === (Right(()))
      failure(damaged) should include(s"$objectKey (ETag mismatch)")
  }

  test("an ETag that is not computed from the part MD5s is not reported as damage") {
    for
      e <- env
      // SSE-KMSなどでは、ETagが<MD5>-<パートの数>の形でもMD5から計算されていない
      _ <- update(e, objectKey)(_.copy(etag = "\"0123456789abcdef0123456789abcdef-3\""))
      formed <- verify(e)
      _ <- update(e, objectKey)(_.copy(etag = "\"opaque\""))
      opaque <- verify(e)
    yield
      formed should === (Right(()))
      opaque should === (Right(()))
  }

  test("an object in the index but not on S3 is reported as missing") {
    for
      e <- env
      _ <- e.store.objects.update(_ - objectKey)
      r <- verify(e)
    yield failure(r) should include(s"$objectKey (missing)")
  }

  test("--snapshot verifies only that backup and fails when there is none") {
    for
      e <- env
      one <- verify(e, snapshot = Some("daily-1"))
      none <- verify(e, snapshot = Some("daily-2"))
    yield
      one should === (Right(()))
      failure(none) should include(s"no backup of $dataset@daily-2")
  }
