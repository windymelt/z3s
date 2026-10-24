package dev.capslock.z3s

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
  /** ZFSネイティブ暗号化されたdataset */
  val encrypted: Ref[IO, Set[String]] = Ref.unsafe(Set.empty)
  /** zfs sendを失敗させるdataset */
  val failingSends: Ref[IO, Set[String]] = Ref.unsafe(Set.empty)
  /** zfs sendを呼んだ(スナップショット, raw)の組 */
  val sends: Ref[IO, List[(String, Boolean)]] = Ref.unsafe(Nil)

  def listDatasets(root: String): IO[List[DatasetInfo]] =
    (datasets.get, encrypted.get).mapN { (m, enc) =>
      m.keys.toList.filter(n => n == root || n.startsWith(s"$root/")).sorted.map(n => DatasetInfo(n, enc.contains(n)))
    }

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
    Stream.eval(sends.update(_ :+ (snapshot -> raw)) *> find(snapshot) <* failingSends.get.flatMap { failing =>
      val dataset = snapshot.takeWhile(_ != '@')
      IO.raiseError(new Exception(s"injected send failure for $dataset")).whenA(failing.contains(dataset))
    }).flatMap(s => Stream.chunk(Chunk.array(streamFor(s, parent, raw))).rechunkRandomly())

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

  final case class PendingUpload(key: String, metadata: Map[String, String], parts: Map[Int, Array[Byte]])

  val uploads: Ref[IO, Map[String, PendingUpload]] = Ref.unsafe(Map.empty)
  val uploadPartCalls: Ref[IO, List[Int]] = Ref.unsafe(Nil)
  val aborted: Ref[IO, List[String]] = Ref.unsafe(Nil)
  /** この番号以降のUploadPartを失敗させる(中断の再現用) */
  val failPartsFrom: Ref[IO, Option[Int]] = Ref.unsafe(None)

  def createMultipartUpload(key: String, metadata: Map[String, String], storageClass: Option[String]): IO[String] =
    counter.updateAndGet(_ + 1).flatMap { n =>
      val id = s"upload-$n"
      uploads.update(_.updated(id, PendingUpload(key, metadata, Map.empty))).as(id)
    }

  def uploadPart(key: String, uploadId: String, number: Int, bytes: Array[Byte]): IO[String] =
    failPartsFrom.get.flatMap {
      case Some(n) if number >= n => IO.raiseError(new java.io.IOException(s"injected failure for part $number"))
      case _ =>
        uploadPartCalls.update(_ :+ number) *> partSizes.update(_ :+ bytes.length) *>
          uploads.modify { m =>
            m.get(uploadId) match
              case Some(u) => (m.updated(uploadId, u.copy(parts = u.parts.updated(number, bytes.clone()))), true)
              case None    => (m, false)
          }.flatMap(ok =>
            if ok then IO.pure(s"\"part-$number-${bytes.length}\"")
            else IO.raiseError(new Exception(s"no such upload: $uploadId"))
          )
    }

  def listParts(key: String, uploadId: String): IO[Option[List[UploadedPart]]] =
    uploads.get.map(_.get(uploadId).map(_.parts.toList.sortBy(_._1).map { (n, b) =>
      UploadedPart(n, s"\"part-$n-${b.length}\"", b.length.toLong)
    }))

  def completeMultipartUpload(key: String, uploadId: String, parts: List[UploadedPart]): IO[String] =
    nextEtag.flatMap { etag =>
      uploads.get.map(_.get(uploadId)).flatMap {
        case None => IO.raiseError(new Exception(s"no such upload: $uploadId"))
        case Some(u) =>
          val bytes = parts.sortBy(_.number).toArray.flatMap(p => u.parts(p.number))
          objects
            .modify { m =>
              if m.contains(key) then (m, false)
              else (m.updated(key, StoredObject(bytes, etag, Instant.EPOCH, u.metadata)), true)
            }
            .flatMap(ok =>
              if ok then uploads.update(_ - uploadId).as(etag)
              else IO.raiseError(Z3SError.Unresumable(s"object already exists: $key"))
            )
      }
    }

  def abortMultipartUpload(key: String, uploadId: String): IO[Unit] =
    aborted.update(_ :+ uploadId) *> uploads.update(_ - uploadId)
