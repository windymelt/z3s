package dev.capslock.z3s.age

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.{Chunk, Stream}

import scala.util.Random

object AgeTestSupport:
  def randomBytes(n: Int, seed: Long = 42L): Array[Byte] =
    val a = new Array[Byte](n)
    Random(seed).nextBytes(a)
    a

  def encryptBytes(recipients: NonEmptyList[X25519Recipient], plain: Array[Byte], chunk: Int = 8192): IO[Array[Byte]] =
    Stream.chunk(Chunk.array(plain)).chunkLimit(chunk).unchunks.through(Age.encrypt(recipients)).compile.to(Array)

  def decryptBytes(identities: NonEmptyList[X25519Identity], cipher: Array[Byte], chunk: Int = 8192): IO[Array[Byte]] =
    Stream.chunk(Chunk.array(cipher)).chunkLimit(chunk).unchunks.through(Age.decrypt(identities)).compile.to(Array)

  /** ヘッダ（MAC行の改行まで）のバイト数 */
  def headerLength(cipher: Array[Byte]): Int =
    val macLine = cipher.indexOfSlice("\n--- ".getBytes("US-ASCII"))
    cipher.indexOf('\n'.toByte, macLine + 1) + 1
