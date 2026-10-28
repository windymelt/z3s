package dev.capslock.z3s.codec

import io.bullet.borer.{Reader, Writer}

/** codecが共有する読み書きの補助関数。CBORとJSONのどちらにも使う */
private[codec] object BorerSupport:
  def fail(msg: String): Nothing = throw new IllegalArgumentException(msg)

  /** 符号なし64ビット値を`Long`のビット列のまま書く */
  def writeU64(w: Writer, v: Long): Writer =
    if v >= 0 then w.writeLong(v) else w.writeOverLong(negative = false, v)

  def readU64(r: Reader): Long =
    if r.hasOverLong then r.readOverLong()
    else
      val v = r.readLong()
      if v < 0 then fail(s"expected unsigned integer, got $v") else v

  /** mapの各要素を読み、`known`が扱わなかったキーの値を読み飛ばす */
  def readMap(r: Reader)(known: PartialFunction[String, Reader => Unit]): Unit =
    def field(): Unit =
      val key = r.readString()
      known.lift(key) match
        case Some(read) => read(r)
        case None       => r.skipElement()
    if r.hasMapHeader then
      val n = r.readMapHeader()
      var i = 0L
      while i < n do
        field()
        i += 1
    else
      r.readMapStart()
      while !r.tryReadBreak() do field()

  def readArray[A](r: Reader)(elem: Reader => A): List[A] =
    val b = List.newBuilder[A]
    if r.hasArrayHeader then
      val n = r.readArrayHeader()
      var i = 0L
      while i < n do
        b += elem(r)
        i += 1
    else
      r.readArrayStart()
      while !r.tryReadBreak() do b += elem(r)
    b.result()

  def required[A](name: String, v: Option[A]): A = v.getOrElse(fail(s"missing key: $name"))
