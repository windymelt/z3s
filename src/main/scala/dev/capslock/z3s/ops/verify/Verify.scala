package dev.capslock.z3s.ops.verify

import cats.effect.IO
import cats.effect.std.Random
import cats.effect.syntax.all.*
import cats.syntax.all.*
import dev.capslock.z3s.*
import dev.capslock.z3s.codec.partdigests.PartDigestsCodec
import dev.capslock.z3s.ops.{OpsContext, Tree}
import dev.capslock.z3s.ops.reindex.Reindex

/** @param sample Someなら、各オブジェクトからこの数のパートを無作為に選んで読む。Noneなら全体を読む */
final case class VerifyOptions(snapshot: Option[String], sample: Option[Int], recursive: Boolean = false)

/** 1つのオブジェクトを検証した結果 */
enum Verdict:
  case Ok(detail: String)
  /** 記録したMD5と一致しなかったパートの番号(1から数える) */
  case Corrupt(parts: List[Int])
  /** 読んだ内容から計算したETagがサーバのETagと一致せず、どのパートかは特定できない */
  case Mismatch(computed: MultipartEtag, etag: MultipartEtag)
  case Unverifiable(reason: String)
  case Missing

  /** 検証に失敗したことを表す短い説明。検証できた、または検証できなかった場合はNoneを返す */
  def problem: Option[String] = this match
    case Corrupt(parts)          => Some(s"corrupt part(s) ${parts.mkString(", ")}")
    case _: Mismatch             => Some("ETag mismatch")
    case Missing                 => Some("missing")
    case _: Ok | _: Unverifiable => None

/** verifyサブコマンドを処理する。オブジェクトを読んで各パートのMD5を計算し、サーバのETagと照合する */
private[ops] final class Verify(ctx: OpsContext):
  import ctx.{common, err, out, store}

  def verify(opts: VerifyOptions): IO[Unit] =
    if opts.recursive then verifyTree(opts) else verifyDataset(opts, inTree = false)

  /** S3にあるdatasetの木を順に検証する。失敗したdatasetがあっても残りを続け、最後にまとめて報告する */
  private def verifyTree(opts: VerifyOptions): IO[Unit] =
    for
      datasets <- Tree.remoteDatasets(ctx)
      _ <- IO.raiseError(Z3SError.Runtime(s"no backups of ${common.dataset} or its descendants on S3")).whenA(datasets.isEmpty)
      _ <- out(s"recursive verify of ${datasets.size} dataset(s)")
      results <- datasets.zipWithIndex.traverse { (ds, i) =>
        out(s"== [${i + 1}/${datasets.size}] $ds") *>
          Verify(ctx.forDataset(ds)).verifyDataset(opts, inTree = true).attempt.flatMap {
            case Right(()) => IO.pure(None)
            case Left(e)   => err(s"error: $ds: ${e.getMessage}").as(Some((dataset = ds, error = e)))
          }
      }
      _ <- Tree.raiseFailures(results.flatten, Nil, datasets.size)
    yield ()

  /** @param inTree trueなら、--snapshotのバックアップがないdatasetを警告して飛ばす */
  private def verifyDataset(opts: VerifyOptions, inTree: Boolean): IO[Unit] =
    val wanted = opts.snapshot.map(s => s"${common.dataset}@$s")
    for
      index <- Reindex(ctx).loadOrCreateIndex
      entries = index.entries.filter(e => wanted.forall(_ == e.header.snapshot)).sortBy(_.header.createtxg)
      _ <- (wanted, entries) match
        case (Some(s), Nil) if inTree => err(s"warning: no backup of $s; skipping")
        case (Some(s), Nil)           => IO.raiseError(Z3SError.Runtime(s"no backup of $s in the index"))
        case (None, Nil)              => out(s"no backups of ${common.dataset} on S3")
        case _                        => IO.unit
      verdicts <- entries.zipWithIndex.traverse { (e, i) =>
        verifyEntry(e, opts.sample, s"[${i + 1}/${entries.size}]").flatTap(report(e, _)).tupleLeft(e)
      }
      problems = verdicts.flatMap((e, v) => v.problem.map(p => s"${e.key} ($p)"))
      _ <- IO
        .raiseError(
          Z3SError.Runtime(s"${problems.size} of ${entries.size} object(s) failed verification: ${problems.mkString(", ")}")
        )
        .whenA(problems.nonEmpty)
    yield ()

  private def verifyEntry(entry: IndexEntry, sample: Option[Int], position: String): IO[Verdict] =
    store.head(entry.key).flatMap {
      case None => IO.pure(Verdict.Missing)
      case Some(info) =>
        MultipartEtag.parse(info.etag) match
          case None => IO.pure(Verdict.Unverifiable(s"the ETag ${info.etag} is not of the form <MD5>-<parts>"))
          case Some(etag) =>
            loadPartDigests(entry).flatMap { digests =>
              // パートの大きさは、一覧、ヘッダ、古いヘッダのための見積もりからの計算の順に探す
              val partSize = digests
                .map(_.partSize)
                .orElse(entry.header.partSize)
                .getOrElse(Planner.partSize(entry.header.estimatedSize))
              // サーバのETagと一致する一覧だけを、各パートの正しいMD5として使う
              val trusted = digests.filter(d => d.size == info.size && MultipartEtag.of(d.partMd5s) == etag)
              if MultipartEtag.partCount(info.size, partSize) != etag.parts then
                IO.pure(Verdict.Unverifiable(s"$partSize-byte parts do not add up to the ${etag.parts} parts in the ETag"))
              else
                sample match
                  case Some(n) => verifySample(entry, info, partSize, n, digests, trusted)
                  case None    => verifyFull(entry, info, partSize, etag, digests, trusted, position)
            }
    }

  /** オブジェクト全体を読み、計算したETagをサーバのETagと比べる */
  private def verifyFull(
      entry: IndexEntry,
      info: ObjectInfo,
      partSize: Long,
      etag: MultipartEtag,
      digests: Option[PartDigests],
      trusted: Option[PartDigests],
      position: String
  ): IO[Verdict] =
    store
      .get(entry.key)
      .through(ctx.progress.track(s"$position ${entry.header.snapshot}", Some(info.size)))
      .through(MultipartEtag.partMd5s(partSize))
      .compile
      .toList
      .map { actual =>
        val computed = MultipartEtag.of(actual)
        if computed == etag then Verdict.Ok(s"${etag.parts} part(s) read")
        else
          trusted match
            case Some(d) => Verdict.Corrupt(differingParts(d.partMd5s, actual))
            case None if digests.exists(_.partMd5s == actual) =>
              Verdict.Unverifiable(
                "the data matches the part MD5s recorded at upload, but not the ETag; " +
                  "the storage may not compute ETags from MD5s"
              )
            case None => Verdict.Mismatch(computed, etag)
      }

  /** 無作為に選んだパートだけをranged GETで読み、記録したMD5と比べる */
  private def verifySample(
      entry: IndexEntry,
      info: ObjectInfo,
      partSize: Long,
      count: Int,
      digests: Option[PartDigests],
      trusted: Option[PartDigests]
  ): IO[Verdict] =
    (digests, trusted) match
      case (None, _) =>
        IO.pure(Verdict.Unverifiable("the part MD5 list was not found; run without --sample to read the whole object"))
      case (Some(_), None) =>
        IO.pure(Verdict.Unverifiable("the part MD5 list does not match the ETag; run without --sample"))
      case (_, Some(d)) =>
        for
          random <- Random.scalaUtilRandom[IO]
          picked <- random.shuffleList(d.partMd5s.indices.toList).map(_.take(count).sorted)
          bad <- picked.parTraverseN(common.concurrency) { i =>
            val offset = i * partSize
            store
              .getRange(entry.key, offset, math.min(partSize, info.size - offset))
              .through(MultipartEtag.partMd5s(partSize))
              .compile
              .toList
              .map(actual => Option.when(actual != List(d.partMd5s(i)))(i + 1))
          }
        yield bad.flatten match
          case Nil   => Verdict.Ok(s"${picked.size} of ${d.partMd5s.size} part(s) sampled")
          case parts => Verdict.Corrupt(parts)

  /** 番号は1から数える。パートの数が違う場合、片方にしかないパートも一致しないものとする */
  private def differingParts(expected: List[Md5], actual: List[Md5]): List[Int] =
    (0 until math.max(expected.size, actual.size)).toList.collect {
      case i if expected.lift(i) != actual.lift(i) => i + 1
    }

  /** 一覧がない、または読めない場合はNoneを返す */
  private def loadPartDigests(entry: IndexEntry): IO[Option[PartDigests]] =
    val key = common.partDigestsKey(Snapshot(entry.header.snapshot, entry.header.guid, entry.header.createtxg))
    store.getWithEtag(key).flatMap {
      case None => IO.pure(None)
      case Some((bytes, _)) =>
        PartDigestsCodec.decode(bytes) match
          case Right(d) => IO.pure(Some(d))
          case Left(e)  => err(s"warning: ignoring $key: $e").as(None)
    }

  private def report(entry: IndexEntry, verdict: Verdict): IO[Unit] = verdict match
    case Verdict.Ok(detail) => out(s"ok: ${entry.key} ($detail)")
    case Verdict.Corrupt(parts) =>
      err(s"corrupt: ${entry.key}: part(s) ${parts.mkString(", ")} differ from the recorded MD5s")
    case Verdict.Mismatch(computed, etag) =>
      err(
        s"mismatch: ${entry.key}: the computed ETag ${computed.show} differs from ${etag.show}; " +
          "the object is damaged, or the storage does not compute ETags from MD5s"
      )
    case Verdict.Unverifiable(reason) => err(s"warning: cannot verify ${entry.key}: $reason")
    case Verdict.Missing              => err(s"missing: ${entry.key} is in the index but not on S3")
