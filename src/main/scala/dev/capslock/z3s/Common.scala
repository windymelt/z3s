package dev.capslock.z3s

val IndexFileName = ".z3s-index.cbor"
val PartDigestsDirName = ".z3s-parts"

final case class Common(
    bucket: String,
    prefix: String,
    dataset: String,
    snapshotPrefix: Option[String],
    dryRun: Boolean,
    concurrency: Int,
    progress: ProgressMode = ProgressMode.None
):
  def objectKey(snapshotName: String): String = s"$prefix$snapshotName"
  def objectPrefix: String = s"$prefix$dataset@"
  def indexKey: String = s"$prefix$dataset/$IndexFileName"
  /** バックアップの一覧(objectPrefix)に入らないよう、indexと同じくdatasetの下に置く */
  def partDigestsKey(snapshot: Snapshot): String = s"$prefix${snapshot.dataset}/$PartDigestsDirName/${snapshot.shortName}.cbor"
