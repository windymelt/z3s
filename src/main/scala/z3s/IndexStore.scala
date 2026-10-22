package z3s

import cats.effect.IO
import cats.syntax.all.*

final class IndexStore(store: ObjectStore, val key: String):
  private val MaxAttempts = 5

  def load: IO[Option[(Index, String)]] =
    store.getWithEtag(key).flatMap {
      case None => IO.pure(None)
      case Some((bytes, etag)) =>
        Cbor.decodeIndex(bytes).leftMap(Z3SError.Runtime(_)).liftTo[IO].map(i => Some((i, etag)))
    }

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
