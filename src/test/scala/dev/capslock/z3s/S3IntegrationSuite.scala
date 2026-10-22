package dev.capslock.z3s

import cats.effect.{IO, Ref, Resource}
import fs2.{Chunk, Stream}
import java.net.URI
import munit.CatsEffectSuite
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, ListMultipartUploadsRequest}

/** S3互換サーバに対する結合テスト。Z3S_IT_ENDPOINTが設定されている場合だけ実行する
  *
  * Z3S_IT_ENDPOINT=http://127.0.0.1:9000 Z3S_IT_ACCESS_KEY=... Z3S_IT_SECRET_KEY=... sbt "testOnly dev.capslock.z3s.S3IntegrationSuite"
  */
class S3IntegrationSuite extends CatsEffectSuite:
  override def munitIOTimeout = scala.concurrent.duration.Duration(5, "min")

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

  val data: Array[Byte] =
    val rnd = new scala.util.Random(1)
    Array.fill(12 * 1024 * 1024 + 123)(rnd.nextInt(256).toByte)

  test("conditional PUT") {
    withStore { (_, store, _) =>
      for
        a <- store.putIfAbsent("k", Array[Byte](1))
        b <- store.putIfAbsent("k", Array[Byte](2))
        etag = a match { case PutResult.Written(e) => e; case _ => fail("not written") }
        c <- store.putIfMatch("k", Array[Byte](3), "\"0123456789abcdef0123456789abcdef\"")
        d <- store.putIfMatch("k", Array[Byte](4), etag)
        got <- store.getWithEtag("k")
        missing <- store.getWithEtag("nope")
      yield
        assertEquals(b, PutResult.PreconditionFailed)
        assertEquals(c, PutResult.PreconditionFailed)
        assert(d.isInstanceOf[PutResult.Written])
        assertEquals(got.map(_._1.toList), Some(List[Byte](4)))
        assertEquals(missing, None)
    }
  }

  test("multipart upload, range GET and streaming GET") {
    withStore { (c, store, bucket) =>
      val src = Stream.chunk(Chunk.array(data)).covary[IO].rechunkRandomly()
      for
        r <- store.uploadMultipart("obj", Map("z3s-format" -> "1"), None, 5 * 1024 * 1024, 2, src)
        head <- store.head("obj")
        prefix <- store.getPrefix("obj", 100)
        whole <- store.get("obj").compile.to(Array)
        listed <- store.list("o").compile.toList
        again <- store.uploadMultipart("obj", Map.empty, None, 5 * 1024 * 1024, 2, src).attempt
        pending <- pendingUploads(c, bucket)
      yield
        assertEquals(r.size, data.length.toLong)
        assertEquals(head.map(_.size), Some(data.length.toLong))
        assertEquals(prefix.toList, data.take(100).toList)
        assert(java.util.Arrays.equals(whole, data))
        assertEquals(listed.map(_.key), List("obj"))
        assert(again.left.exists(_.getMessage.contains("already exists")), again.toString)
        assertEquals(pending, 0)
    }
  }

  test("a failing source aborts the multipart upload") {
    withStore { (c, store, bucket) =>
      val src = Stream.chunk(Chunk.array(data)).covary[IO] ++ Stream.raiseError[IO](new Exception("boom"))
      for
        r <- store.uploadMultipart("obj", Map.empty, None, 5 * 1024 * 1024, 2, src).attempt
        head <- store.head("obj")
        pending <- pendingUploads(c, bucket)
      yield
        assert(r.isLeft)
        assertEquals(head, None)
        assertEquals(pending, 0)
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
      yield
        assertEquals(index.entries.map(_.key), List("p/tank/data@a", "p/tank/data@b"))
        assertEquals(got.map(_._2.toList), List(
          zfs.streamFor(snapshots(0), None, false).toList,
          zfs.streamFor(snapshots(1), Some("tank/data@a"), false).toList
        ))
    }
  }
