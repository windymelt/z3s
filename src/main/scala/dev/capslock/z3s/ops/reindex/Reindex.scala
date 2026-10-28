package dev.capslock.z3s.ops.reindex

import cats.effect.IO
import cats.syntax.all.*
import dev.capslock.z3s.*
import dev.capslock.z3s.codec.header.HeaderCodec
import dev.capslock.z3s.ops.OpsContext
import scodec.bits.ByteVector

/** S3上のオブジェクトのヘッダからインデックスを作り直す */
private[ops] final class Reindex(ctx: OpsContext):
  import ctx.{common, err, out, store}

  def reindex: IO[Index] =
    val scan: IO[Index] = store
      .list(common.objectPrefix)
      .parEvalMap(common.concurrency)(readEntry)
      // 読めなかったオブジェクトはreadEntryがNoneにするので、ここで除く
      .unNone
      .compile
      .toList
      .map(entries => Index(Index.SchemaVersion, entries.sortBy(_.header.createtxg)))
    if common.dryRun then
      scan.flatTap(i => out(s"[dry-run] would write index with ${i.entries.size} entries to ${common.indexKey}"))
    else ctx.indexStore.replace(scan)

  /** インデックスを読む。なければ作り直す */
  def loadOrCreateIndex: IO[Index] =
    ctx.indexStore.load.flatMap {
      case Some((index, _)) => IO.pure(index)
      case None             => err(s"index ${common.indexKey} not found; running reindex") *> reindex
    }

  private def readEntry(o: ObjectInfo): IO[Option[IndexEntry]] =
    store.getPrefix(o.key, Frame.MaxFrameLength).flatMap { bytes =>
      val frame: Either[String, Frame] = Frame.codec
        .decode(ByteVector(bytes).bits)
        .toEither
        .leftMap(_.messageWithContext)
        .map(_.value)
      val header = frame.flatMap { f =>
        if f.version == Frame.CurrentVersion then HeaderCodec.decode(f.header.toArray)
        else Left(s"unsupported format version ${f.version}")
      }
      header match
        case Right(h) => IO.pure(Some(IndexEntry(o.key, h, o.size, o.etag, o.lastModified.toString)))
        case Left(e)  => err(s"warning: skipping ${o.key}: $e").as(None)
    }
