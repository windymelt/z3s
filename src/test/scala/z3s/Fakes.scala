package z3s

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

/** sendは先頭行にスナップショットの名前とguidとcreatetxgを書き、recvはその行を読んでスナップショットを登録する */
final class FakeZfs(
    val datasets: Ref[IO, Map[String, List[Snapshot]]],
    payloadSize: Int
) extends Zfs:
  val received: Ref[IO, List[(String, Array[Byte])]] = Ref.unsafe(Nil)

  def streamFor(snapshot: Snapshot, parent: Option[String], raw: Boolean): Array[Byte] =
    val head = s"SNAP ${snapshot.shortName} ${snapshot.guid} ${snapshot.createtxg} ${parent.getOrElse("-")} $raw\n"
    val rnd = new scala.util.Random(snapshot.guid ^ parent.hashCode)
    head.getBytes(UTF_8) ++ Array.fill(payloadSize)((rnd.nextInt(16) + 'a').toByte)

  private def find(name: String): IO[Snapshot] =
    datasets.get.map(_.values.flatten.find(_.name == name)).flatMap(_.liftTo[IO](new Exception(s"no $name")))

  def listSnapshots(dataset: String): IO[List[Snapshot]] =
    datasets.get.map(_.get(dataset)).flatMap(_.liftTo[IO](new Exception(s"no dataset $dataset")))

  def datasetExists(dataset: String): IO[Boolean] = datasets.get.map(_.contains(dataset))

  def estimateSize(snapshot: String, parent: Option[String], raw: Boolean): IO[Long] =
    find(snapshot).map(s => streamFor(s, parent, raw).length.toLong)

  def send(snapshot: String, parent: Option[String], raw: Boolean): Stream[IO, Byte] =
    Stream.eval(find(snapshot)).flatMap(s => Stream.chunk(Chunk.array(streamFor(s, parent, raw))).rechunkRandomly())

  def recv(target: String, force: Boolean, input: Stream[IO, Byte]): IO[Unit] =
    input.compile.to(Array).flatMap { bytes =>
      val line = new String(bytes.takeWhile(_ != '\n'.toByte), UTF_8)
      line.split(" ") match
        case Array("SNAP", short, guid, txg, _, _) =>
          val s = Snapshot(s"$target@$short", guid.toLong, txg.toLong)
          received.update(_ :+ (target -> bytes)) *>
            datasets.update(m => m.updated(target, m.getOrElse(target, Nil) :+ s))
        case _ => IO.raiseError(new Exception(s"bad stream: $line"))
    }

final case class StoredObject(bytes: Array[Byte], etag: String, lastModified: Instant, metadata: Map[String, String])

final class InMemoryStore extends ObjectStore:
  val objects: Ref[IO, Map[String, StoredObject]] = Ref.unsafe(Map.empty)
  private val counter: Ref[IO, Int] = Ref.unsafe(0)
  val partSizes: Ref[IO, List[Int]] = Ref.unsafe(Nil)
  /** 競合を再現するため、次の条件付きPUTの直前に実行する処理 */
  val beforePut: Ref[IO, Option[IO[Unit]]] = Ref.unsafe(None)

  private def nextEtag: IO[String] = counter.updateAndGet(_ + 1).map(n => s"\"etag-$n\"")

  def put(key: String, bytes: Array[Byte]): IO[Unit] =
    nextEtag.flatMap(etag => objects.update(_.updated(key, StoredObject(bytes, etag, Instant.EPOCH, Map.empty))))

  def list(prefix: String): Stream[IO, ObjectInfo] =
    Stream.evalSeq(objects.get.map(_.toList.sortBy(_._1).collect {
      case (k, o) if k.startsWith(prefix) => ObjectInfo(k, o.bytes.length.toLong, o.etag, o.lastModified)
    }))

  def head(key: String): IO[Option[ObjectInfo]] =
    objects.get.map(_.get(key).map(o => ObjectInfo(key, o.bytes.length.toLong, o.etag, o.lastModified)))

  def getPrefix(key: String, length: Int): IO[Array[Byte]] =
    objects.get.map(_(key).bytes.take(length))

  def get(key: String): Stream[IO, Byte] =
    Stream.eval(objects.get.map(_(key).bytes)).flatMap(b => Stream.chunk(Chunk.array(b))).rechunkRandomly()

  def getWithEtag(key: String): IO[Option[(Array[Byte], String)]] =
    objects.get.map(_.get(key).map(o => (o.bytes, o.etag)))

  private def conditionalPut(key: String, bytes: Array[Byte])(ok: Option[StoredObject] => Boolean): IO[PutResult] =
    beforePut.getAndSet(None).flatMap(_.sequence_) *>
      nextEtag.flatMap { etag =>
        objects.modify { m =>
          if ok(m.get(key)) then
            (m.updated(key, StoredObject(bytes, etag, Instant.EPOCH, Map.empty)), PutResult.Written(etag))
          else (m, PutResult.PreconditionFailed)
        }
      }

  def putIfAbsent(key: String, bytes: Array[Byte]): IO[PutResult] = conditionalPut(key, bytes)(_.isEmpty)

  def putIfMatch(key: String, bytes: Array[Byte], etag: String): IO[PutResult] =
    conditionalPut(key, bytes)(_.exists(_.etag == etag))

  def uploadMultipart(
      key: String,
      metadata: Map[String, String],
      storageClass: Option[String],
      partSize: Int,
      concurrency: Int,
      data: Stream[IO, Byte]
  ): IO[UploadResult] =
    data.chunkN(partSize).map(_.toArray).compile.toList.flatMap { parts =>
      partSizes.update(_ ++ parts.map(_.length)) *> nextEtag.flatMap { etag =>
        val bytes = parts.toArray.flatten
        objects
          .modify { m =>
            if m.contains(key) then (m, false)
            else (m.updated(key, StoredObject(bytes, etag, Instant.EPOCH, metadata)), true)
          }
          .flatMap(ok =>
            if ok then IO.pure(UploadResult(etag, bytes.length.toLong))
            else IO.raiseError(Z3SError.Runtime(s"object already exists: $key"))
          )
      }
    }
