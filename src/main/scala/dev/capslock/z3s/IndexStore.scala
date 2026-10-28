package dev.capslock.z3s

import cats.effect.IO
import cats.syntax.all.*
import dev.capslock.z3s.codec.Cbor

final class IndexStore(store: ObjectStore, val key: String):
  private val MaxAttempts = 5

  def load: IO[Option[(Index, String)]] =
    store.getWithEtag(key).flatMap {
      case None => IO.pure(None)
      case Some((bytes, etag)) =>
        Cbor.decodeIndex(bytes).leftMap(Z3SError.Runtime(_)).liftTo[IO].map(i => Some((i, etag)))
    }

  /** 現在のインデックスを読まずに置き換える。`build`の実行前に取得したETagを条件に書き込み、
    * 412の場合は`build`からやり直す。壊れたインデックスも置き換えられる
    */
  def replace(build: IO[Index]): IO[Index] =
    def attempt(n: Int): IO[Index] =
      store.head(key).map(_.map(_.etag)).flatMap { before =>
        build.flatMap { next =>
          val bytes = Cbor.encodeIndex(next)
          before.fold(store.putIfAbsent(key, bytes))(store.putIfMatch(key, bytes, _)).flatMap {
            case PutResult.Written(_) => IO.pure(next)
            case PutResult.PreconditionFailed if n < MaxAttempts =>
              IO.consoleForIO.errorln(s"index was updated during reindex; rescanning ($n/$MaxAttempts)") *>
                attempt(n + 1)
            case PutResult.PreconditionFailed =>
              IO.raiseError(Z3SError.Runtime(s"failed to replace index after $MaxAttempts attempts"))
          }
        }
      }
    attempt(1)

  /** 読んだ時点のETagを条件に書き込む。412の場合は読み直して`f`を適用し直す */
  def update(f: Index => Index): IO[Index] =
    def attempt(n: Int): IO[Index] =
      load.flatMap { current =>
        val next = f(current.fold(Index.empty)(_._1))
        val bytes = Cbor.encodeIndex(next)
        val put = current match
          case None            => store.putIfAbsent(key, bytes)
          case Some((_, etag)) => store.putIfMatch(key, bytes, etag)
        put.flatMap {
          case PutResult.Written(_) => IO.pure(next)
          case PutResult.PreconditionFailed if n < MaxAttempts =>
            IO.consoleForIO.errorln(s"index was updated concurrently; retrying ($n/$MaxAttempts)") *> attempt(n + 1)
          case PutResult.PreconditionFailed =>
            IO.raiseError(Z3SError.Runtime(s"failed to update index after $MaxAttempts attempts"))
        }
      }
    attempt(1)
