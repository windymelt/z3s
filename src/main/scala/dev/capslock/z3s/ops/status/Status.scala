package dev.capslock.z3s.ops.status

import cats.effect.IO
import cats.syntax.all.*
import dev.capslock.z3s.*
import dev.capslock.z3s.ops.{OpsContext, Tree}
import dev.capslock.z3s.ops.reindex.Reindex
import io.bullet.borer.{Json, Writer}

/** statusサブコマンドを処理する。S3にあるバックアップを一覧にし、各バックアップの健全性と、同じスナップショットがローカルにあるかを表示する */
private[ops] final class Status(ctx: OpsContext):
  import ctx.{common, zfs}

  def status(refresh: Boolean, json: Boolean, recursive: Boolean = false): IO[Unit] =
    statusText(refresh, json, recursive).flatMap(ctx.out)

  def statusText(refresh: Boolean, json: Boolean, recursive: Boolean): IO[String] =
    if !recursive then statusRows(refresh).map(rows => if json then StatusRow.json(rows) else StatusRow.table(rows))
    else
      for
        datasets <- treeDatasets
        sections <- datasets.traverse(ds => Status(ctx.forDataset(ds)).statusRows(refresh).map(ds -> _))
      yield if json then StatusRow.jsonTree(sections) else StatusRow.tableTree(sections)

  private def statusRows(refresh: Boolean): IO[List[StatusRow]] =
    val reindex = Reindex(ctx)
    for
      index <- if refresh then reindex.reindex else reindex.loadOrCreateIndex
      exists <- zfs.datasetExists(common.dataset)
      locals <- if exists then zfs.listSnapshots(common.dataset) else IO.pure(Nil)
      health = Planner.health(index)
      localGuids = locals.map(_.guid).toSet
    yield index.entries.sortBy(_.header.createtxg).map { e =>
      StatusRow(e, health(e.key), localGuids.contains(e.header.guid))
    }

  /** ローカルの子孫と、S3にバックアップかインデックスがあるdatasetを合わせて、名前の順に返す */
  private def treeDatasets: IO[List[String]] =
    for
      exists <- zfs.datasetExists(common.dataset)
      local <- if exists then zfs.listDatasets(common.dataset).map(_.map(_.name)) else IO.pure(Nil)
      remote <- Tree.remoteDatasets(ctx)
    yield (local ++ remote).distinct.sorted

final case class StatusRow(entry: IndexEntry, health: Health, local: Boolean):
  def h: Header = entry.header
  def kind: String = if h.isFull then "full" else "incremental"
  def mode: String = h.mode match
    case SendMode.Raw         => "raw"
    case SendMode.Plain(c, e) => s"plain(${c.label}, ${e.label})"

object StatusRow:
  def table(rows: List[StatusRow]): String =
    val header = List("SNAPSHOT", "PARENT", "KIND", "MODE", "HEALTH", "LOCAL", "ESTIMATED", "SIZE")
    val body = rows.map(r =>
      List(
        r.h.snapshot,
        r.h.parent.fold("-")(_.name),
        r.kind,
        r.mode,
        r.health.label,
        if r.local then "yes" else "no",
        r.h.estimatedSize.toString,
        r.entry.size.toString
      )
    )
    val all = header :: body
    val widths = header.indices.map(i => all.map(_(i).length).max)
    all.map(_.zip(widths).map((s, w) => s.padTo(w, ' ')).mkString("  ").stripTrailing).mkString("\n")

  def tableTree(sections: List[(String, List[StatusRow])]): String =
    sections
      .map((dataset, rows) => s"== $dataset\n" + (if rows.isEmpty then "(no backups)" else table(rows)))
      .mkString("\n\n")

  def jsonTree(sections: List[(String, List[StatusRow])]): String =
    sections
      .map((dataset, rows) => s"""{"dataset":${Json.encode(dataset).toUtf8String},"snapshots":${json(rows)}}""")
      .mkString("[", ",", "]")

  def json(rows: List[StatusRow]): String =
    def row(w: Writer, r: StatusRow): Writer =
      w.writeMapStart()
      w.writeString("snapshot").writeString(r.h.snapshot)
      w.writeString("guid").writeString(showGuid(r.h.guid))
      w.writeString("parent")
      r.h.parent.fold(w.writeNull())(p => w.writeString(p.name))
      w.writeString("kind").writeString(r.kind)
      w.writeString("mode").writeString(r.h.mode.kind.label)
      w.writeString("compression")
      r.h.mode match
        case SendMode.Plain(c, _) => w.writeString(c.label)
        case SendMode.Raw         => w.writeNull()
      w.writeString("encryption")
      r.h.mode match
        case SendMode.Plain(_, e) => w.writeString(e match { case Encryption.None => "none"; case _ => "age" })
        case SendMode.Raw         => w.writeNull()
      w.writeString("health").writeString(r.health.label)
      w.writeString("local").writeBoolean(r.local)
      w.writeString("estimatedSize").writeLong(r.h.estimatedSize)
      w.writeString("objectSize").writeLong(r.entry.size)
      w.writeBreak()

    given io.bullet.borer.Encoder[List[StatusRow]] = io.bullet.borer.Encoder { (w, rs) =>
      // borerのJSON出力は長さ付きのarrayとmapに対応しないため、不定長で書く
      w.writeArrayStart()
      rs.foreach(row(w, _))
      w.writeBreak()
    }
    Json.encode(rows).toUtf8String
