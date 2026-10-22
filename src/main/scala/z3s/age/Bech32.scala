package z3s.age

/** BIP 173のBech32（bech32mではない）。ageに合わせて90文字の長さ制限は設けない */
private[age] object Bech32:
  private val Charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
  private val Generator = Array(0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3)

  private def polymod(values: Iterable[Int]): Int =
    values.foldLeft(1) { (chk, v) =>
      val top = chk >>> 25
      var c = ((chk & 0x1ffffff) << 5) ^ v
      for i <- 0 until 5 do if ((top >>> i) & 1) != 0 then c ^= Generator(i)
      c
    }

  private def hrpExpand(hrp: String): Vector[Int] =
    hrp.map(_.toInt >>> 5).toVector ++ Vector(0) ++ hrp.map(_.toInt & 31)

  private def convertBits(data: Seq[Int], from: Int, to: Int, pad: Boolean): Either[String, Vector[Int]] =
    val maxv = (1 << to) - 1
    var acc = 0
    var bits = 0
    val out = Vector.newBuilder[Int]
    for v <- data do
      acc = (acc << from) | v
      bits += from
      while bits >= to do
        bits -= to
        out += (acc >>> bits) & maxv
    if pad then
      if bits > 0 then out += (acc << (to - bits)) & maxv
      Right(out.result())
    else if bits >= from then Left("excess padding")
    else if ((acc << (to - bits)) & maxv) != 0 then Left("non-zero padding")
    else Right(out.result())

  /** 小文字で符号化する。大文字が必要な場合は呼び出し側で変換する */
  def encode(hrp: String, data: Array[Byte]): String =
    val lowerHrp = hrp.toLowerCase
    val values = convertBits(data.toSeq.map(_ & 0xff), 8, 5, pad = true).fold(e => throw IllegalStateException(e), identity)
    val pm = polymod(hrpExpand(lowerHrp) ++ values ++ Vector.fill(6)(0)) ^ 1
    val checksum = (0 until 6).map(i => (pm >>> (5 * (5 - i))) & 31)
    lowerHrp + "1" + (values ++ checksum).map(Charset).mkString

  /** hrpは入力どおりの大小文字で返す */
  def decode(s: String): Either[String, (String, Array[Byte])] =
    if s.toLowerCase != s && s.toUpperCase != s then Left("mixed case")
    else
      val pos = s.lastIndexOf('1')
      if pos < 1 || pos + 7 > s.length then Left("separator '1' at invalid position")
      else
        val hrp = s.take(pos)
        if hrp.exists(c => c < 33 || c > 126) then Left("invalid character in human-readable part")
        else
          val lower = s.toLowerCase
          val values = lower.drop(pos + 1).map(Charset.indexOf(_))
          if values.contains(-1) then Left("invalid character in data part")
          else if polymod(hrpExpand(lower.take(pos)) ++ values) != 1 then Left("invalid checksum")
          else convertBits(values.dropRight(6), 5, 8, pad = false).map(bytes => (hrp, bytes.map(_.toByte).toArray))
