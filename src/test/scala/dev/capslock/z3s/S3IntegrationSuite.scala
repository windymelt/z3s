package dev.capslock.z3s

import cats.effect.{IO, Ref, Resource}
import dev.capslock.z3s.cbor.partdigests.PartDigestsCodec
import fs2.{Chunk, Stream}
import java.net.URI
import java.nio.file.Files
import scala.concurrent.duration.*
import scodec.bits.ByteVector
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, ListMultipartUploadsRequest, S3Exception}

/** S3互換サーバに対する結合テスト。Z3S_IT_ENDPOINTが設定されている場合だけ実行する
  *
  * Z3S_IT_ENDPOINT=http://127.0.0.1:9000 Z3S_IT_ACCESS_KEY=... Z3S_IT_SECRET_KEY=... sbt "testOnly dev.capslock.z3s.S3IntegrationSuite"
  */
class S3IntegrationSuite extends IOSpec:

  val endpoint = sys.env.get("Z3S_IT_ENDPOINT")

  val client: Resource[IO, S3AsyncClient] = Resource.fromAutoCloseable(IO {
    S3AsyncClient
      .builder()
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create(endpoint.get))
      .forcePathStyle(true)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(sys.env("Z3S_IT_ACCESS_KEY"), sys.env("Z3S_IT_SECRET_KEY"))
        )
      )
      .build()
  })

  def withStore[A](f: (S3AsyncClient, S3ObjectStore, String) => IO[A]): IO[A] =
    assume(endpoint.isDefined, "Z3S_IT_ENDPOINT is not set")
    val bucket = s"z3s-it-${System.nanoTime()}"
    client.use { c =>
      IO.fromCompletableFuture(IO(c.createBucket(CreateBucketRequest.builder().bucket(bucket).build()))) *>
        f(c, S3ObjectStore(c, bucket), bucket)
    }

  def pendingUploads(c: S3AsyncClient, bucket: String): IO[Int] =
    IO.fromCompletableFuture(IO(c.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(bucket).build())))
      .map(_.uploads().asScala.size)

  val PartSize = 5 * 1024 * 1024

  /** 状態を保存しない(失敗時にabortする)アップロード */
  def upload(store: ObjectStore, key: String, src: Stream[IO, Byte], metadata: Map[String, String] = Map.empty) =
    PartCounters.make.flatMap { counters =>
      MultipartUploader(store, 2, Duration.Zero, _ => IO.unit)
        .upload("bucket", key, metadata, None, PartSize, ByteVector.empty, src, None, None, counters)
    }

  val data: Array[Byte] =
    val rnd = new scala.util.Random(1)
    Array.fill(12 * 1024 * 1024 + 123)(rnd.nextInt(256).toByte)

  test("conditional PUT") {
    withStore { (_, store, _) =>
      for
        created <- store.putIfAbsent("k", Array[Byte](1))
        recreated <- store.putIfAbsent("k", Array[Byte](2))
        etag = created match { case PutResult.Written(e) => e; case _ => fail("not written") }
        staleWrite <- store.putIfMatch("k", Array[Byte](3), "\"0123456789abcdef0123456789abcdef\"")
        matchedWrite <- store.putIfMatch("k", Array[Byte](4), etag)
        got <- store.getWithEtag("k")
        missing <- store.getWithEtag("nope")
      yield
        recreated should === (PutResult.PreconditionFailed)
        staleWrite should === (PutResult.PreconditionFailed)
        matchedWrite shouldBe a[PutResult.Written]
        got.map(_._1.toList) should === (Some(List[Byte](4)))
        missing should === (None)
    }
  }

  test("multipart upload, range GET and streaming GET") {
    withStore { (c, store, bucket) =>
      val src = Stream.chunk(Chunk.array(data)).covary[IO].rechunkRandomly()
      for
        r <- upload(store, "obj", src, Map("z3s-format" -> "1"))
        head <- store.head("obj")
        prefix <- store.getPrefix("obj", 100)
        whole <- store.get("obj").compile.to(Array)
        listed <- store.list("o").compile.toList
        again <- upload(store, "obj", src).attempt
        pending <- pendingUploads(c, bucket)
      yield
        r.size should === (data.length.toLong)
        head.map(_.size) should === (Some(data.length.toLong))
        prefix.toList should === (data.take(100).toList)
        ByteVector(whole) should === (ByteVector(data))
        listed.map(_.key) should === (List("obj"))
        again.left.value.getMessage should include("already exists")
        pending should === (0)
    }
  }

  test("a failing source aborts the multipart upload") {
    withStore { (c, store, bucket) =>
      val src = Stream.chunk(Chunk.array(data)).covary[IO] ++ Stream.raiseError[IO](new Exception("boom"))
      for
        r <- upload(store, "obj", src).attempt
        head <- store.head("obj")
        pending <- pendingUploads(c, bucket)
      yield
        r.left.value.getMessage should === ("boom")
        head should === (None)
        pending should === (0)
    }
  }

  def uploadPart(store: S3ObjectStore, id: String, number: Int, bytes: Array[Byte]): IO[String] =
    MultipartUploader.md5Hex(bytes).flatMap(store.uploadPart("obj", id, number, bytes, _))

  test("UploadPart is rejected when Content-MD5 does not match the part") {
    withStore { (_, store, _) =>
      val part = data.take(PartSize)
      for
        id <- store.createMultipartUpload("obj", Map.empty, None)
        // 空のデータのMD5なので、partとは一致しない
        rejected <- store.uploadPart("obj", id, 1, part, "d41d8cd98f00b204e9800998ecf8427e").attempt
        accepted <- uploadPart(store, id, 1, part)
        _ <- store.abortMultipartUpload("obj", id)
      yield
        rejected.left.toOption.collect { case e: S3Exception => e.awsErrorDetails().errorCode() } should === (Some("BadDigest"))
        accepted should not be empty
    }
  }

  test("ListParts returns uploaded parts, and None for an unknown upload") {
    withStore { (_, store, _) =>
      for
        id <- store.createMultipartUpload("obj", Map.empty, None)
        _ <- uploadPart(store, id, 1, data.take(PartSize))
        _ <- uploadPart(store, id, 2, data.slice(PartSize, PartSize + 10))
        parts <- store.listParts("obj", id)
        _ <- store.abortMultipartUpload("obj", id)
        gone <- store.listParts("obj", id)
      yield
        parts.value.map(p => (p.number, p.size)) should === (List((1, PartSize.toLong), (2, 10L)))
        gone should === (None)
    }
  }

  test("an interrupted upload is resumed from its state file") {
    withStore { (_, store, bucket) =>
      val dir = Files.createTempDirectory("z3s-state")
      val states = StateStore(dir, _ => IO.unit)
      val uploader = MultipartUploader(store, 2, Duration.Zero, _ => IO.unit)
      // 1パート目が状態に記録されるのを待ってから、2パート目の途中で失敗するストリーム
      def waitForFirstPart: IO[Unit] =
        states.load(bucket, "obj").flatMap { s =>
          if s.exists(_.parts.nonEmpty) then IO.unit else IO.sleep(100.millis) *> waitForFirstPart
        }
      val interrupted =
        Stream.chunk(Chunk.array(data.take(PartSize + 100))).covary[IO] ++
          Stream.exec(waitForFirstPart.timeout(30.seconds)) ++ Stream.raiseError[IO](new Exception("cut"))
      val whole = Stream.chunk(Chunk.array(data)).covary[IO]
      for
        c1 <- PartCounters.make
        first <- uploader
          .upload(bucket, "obj", Map.empty, None, PartSize, ByteVector.empty, interrupted, Some(states), None, c1)
          .attempt
        saved <- states.load(bucket, "obj")
        remote <- store.listParts("obj", saved.value.uploadId)
        c2 <- PartCounters.make
        result <- uploader.upload(
          bucket,
          "obj",
          Map.empty,
          None,
          PartSize,
          ByteVector.empty,
          whole,
          Some(states),
          Some(Resume(saved.value, remote.value)),
          c2
        )
        stored <- store.get("obj").compile.to(Array)
        left <- states.load(bucket, "obj")
      yield
        first.left.value.getMessage should === ("cut")
        saved.value.parts.map(_.number) should === (List(1))
        result.size should === (data.length.toLong)
        ByteVector(stored) should === (ByteVector(data))
        left should === (None)
    }
  }

  test("range GET of an empty object returns no bytes") {
    withStore { (_, store, _) =>
      for
        _ <- store.putIfAbsent("empty", Array.emptyByteArray)
        got <- store.getPrefix("empty", 100)
      yield got.length should === (0)
    }
  }

  test("backup, reindex and restore against S3") {
    withStore { (_, store, bucket) =>
      val snapshots = List(Snapshot("tank/data@a", 1, 10), Snapshot("tank/data@b", 2, 20))
      for
        ds <- Ref.of[IO, Map[String, List[Snapshot]]](Map("tank/data" -> snapshots))
        zfs = FakeZfs(ds, 6 * 1024 * 1024)
        ops = Ops(Common(bucket, "p/", "tank/data", None, dryRun = false, concurrency = 2), zfs, store)
        mode = SendMode.Plain(Compression.Zstd(1), Encryption.None)
        _ <- ops.backup(BackupOptions(full = true, Some("a"), mode, None))
        _ <- ops.backup(BackupOptions(full = false, None, mode, None))
        index <- ops.reindex
        _ <- ops.restore(RestoreOptions("b", Some("pool/r"), force = false, None))
        got <- zfs.received.get
        digests <- store.getWithEtag("p/tank/data/.z3s-parts/a.cbor")
        head <- store.head("p/tank/data@a")
      yield
        index.entries.map(_.key) should === (List("p/tank/data@a", "p/tank/data@b"))
        // 保存したパートのMD5一覧から、サーバが返すETagと同じ値を計算できる
        val md5s = PartDigestsCodec.decode(digests.value._1).value.partMd5s
        val joined = md5s.map(_.bytes).reduce(_ ++ _)
        val etag = ByteVector(java.security.MessageDigest.getInstance("MD5").digest(joined.toArray)).toHex
        head.value.etag should === (s"\"$etag-${md5s.size}\"")
        got.map(_._2.toList) should === (List(
          zfs.streamFor(snapshots(0), None, false).toList,
          zfs.streamFor(snapshots(1), Some("tank/data@a"), false).toList
        ))
    }
  }
