package dev.capslock.z3s

/** 終了コードを決めるエラー */
sealed abstract class Z3SError(message: String, val exitCode: Int, cause: Throwable = null)
    extends Exception(message, cause)

object Z3SError:
  /** 実行時エラー(終了コード1) */
  final class Runtime(message: String, cause: Throwable = null) extends Z3SError(message, 1, cause)
  /** 再開しても成功しない実行時エラー(終了コード1)。中断したmultipart uploadを残さない */
  final class Unresumable(message: String, cause: Throwable = null) extends Z3SError(message, 1, cause)
  /** 引数エラー(終了コード2) */
  final class Usage(message: String) extends Z3SError(message, 2)
  /** 健全性チェックによる中断(終了コード3) */
  final class Health(message: String) extends Z3SError(message, 3)

  /** 終了コードに対応する種類のエラーを作る */
  def withExitCode(code: Int, message: String): Z3SError = code match
    case 2 => Usage(message)
    case 3 => Health(message)
    case _ => Runtime(message)
