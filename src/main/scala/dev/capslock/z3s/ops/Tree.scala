package dev.capslock.z3s.ops

import cats.effect.IO
import cats.syntax.all.*
import dev.capslock.z3s.*

/** --recursiveの処理が共有する関数 */
private[ops] object Tree:
  /** 木の処理で失敗したdatasetをまとめて1つのエラーにする。終了コードは、失敗の終了コードがすべて同じならそれを、異なれば1を使う */
  def raiseFailures(failed: List[(String, Throwable)], skipped: List[String], total: Int): IO[Unit] =
    val code = failed.map(_._2).map { case z: Z3SError => z.exitCode; case _ => 1 }.distinct match
      case List(c) => c
      case _       => 1
    val skippedNote =
      if skipped.isEmpty then "" else s"; skipped because an ancestor failed: ${skipped.mkString(", ")}"
    val message = s"${failed.size} of $total dataset(s) failed: ${failed.map(_._1).mkString(", ")}$skippedNote"

    IO.raiseError(Z3SError.fromExitCode(code, message)).whenA(failed.nonEmpty)

  /** rootとその子孫のうち、S3にバックアップかインデックスがあるdatasetを名前の順(親が子より先)に返す */
  def remoteDatasets(ctx: OpsContext): IO[List[String]] =
    val common = ctx.common
    val root = common.dataset
    def isInTree(name: String) = name == root || name.startsWith(s"$root/")

    // prefixの一覧には兄弟のdataset(pool/foobarなど)も含まれるため、isInTreeで絞る
    ctx.store
      .list(s"${common.prefix}$root")
      .map(_.key.stripPrefix(common.prefix))
      // インデックスのキー(<dataset>/.z3s-index.cbor)とバックアップのキー(<dataset>@<snapshot>)から、datasetの名前を取り出す
      .collect {
        case k if k.endsWith(s"/$IndexFileName") => k.stripSuffix(s"/$IndexFileName")
        case k if k.contains('@')                => k.takeWhile(_ != '@')
      }
      .filter(isInTree)
      .compile
      .toList
      .map(_.distinct.sorted)
