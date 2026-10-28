package dev.capslock.z3s.ops

import dev.capslock.z3s.{Common, ObjectStore, Zfs}
import dev.capslock.z3s.ops.backup.Backup
import dev.capslock.z3s.ops.reindex.Reindex
import dev.capslock.z3s.ops.restore.Restore
import dev.capslock.z3s.ops.status.Status
import scala.concurrent.duration.*

export dev.capslock.z3s.ops.backup.BackupOptions
export dev.capslock.z3s.ops.restore.RestoreOptions
export dev.capslock.z3s.ops.status.StatusRow

/** 各サブコマンドの処理の入口 */
final class Ops(common: Common, zfs: Zfs, store: ObjectStore, uploadRetryBase: FiniteDuration = 1.second):
  private val ctx = OpsContext(common, zfs, store, uploadRetryBase)
  private val backupOp = Backup(ctx)
  private val restoreOp = Restore(ctx)
  private val statusOp = Status(ctx)
  private val reindexOp = Reindex(ctx)

  export backupOp.backup
  export restoreOp.restore
  export statusOp.{status, statusText}
  export reindexOp.reindex
