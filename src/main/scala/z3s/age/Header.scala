package z3s.age

import cats.syntax.all.*

import java.nio.charset.StandardCharsets

private[age] final case class Stanza(args: List[String], body: Array[Byte])

/** @param macInput ヘッダ先頭から"---"まで（直後の空白を含まない）の生バイト列 */
private[age] final case class Header(stanzas: List[Stanza], macInput: Array[Byte], mac: Array[Byte])

private[age] object Header:
  val VersionLine = "age-encryption.org/v1"
  private val BodyColumns = 64

  def encodeStanza(stanza: Stanza): String =
    val b64 = Primitives.base64Encode(stanza.body)
    // 最終行は必ず64文字未満にする。ちょうど割り切れる場合は空行で終える
    val lines = b64.grouped(BodyColumns).toList
    val bodyLines = if b64.length % BodyColumns == 0 then lines :+ "" else lines
    ("-> " + stanza.args.mkString(" ") + "\n") + bodyLines.map(_ + "\n").mkString

  /** MACを除いたヘッダ（"---"で終わる）を返す */
  def encodeWithoutMac(stanzas: List[Stanza]): Array[Byte] =
    (VersionLine + "\n" + stanzas.map(encodeStanza).mkString + "---").getBytes(StandardCharsets.US_ASCII)

  private def isArgChar(c: Char): Boolean = c >= 0x21 && c <= 0x7e
  private def isBase64Char(c: Char): Boolean =
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/'

  /** "\n--- ...\n"で終わるヘッダの生バイト列を解釈する */
  def parse(raw: Array[Byte]): Either[String, Header] =
    // ISO-8859-1で1バイト1文字に対応させ、ASCII以外の文字は後段の検査で拒否する
    val text = String(raw, StandardCharsets.ISO_8859_1)
    val lines = text.split("\n", -1).toVector
    if !text.endsWith("\n") then Left("header must end with a newline")
    else if lines.headOption.forall(_ != VersionLine) then Left("unsupported version line")
    else
      // 末尾の改行で生じる空要素を除く
      val content = lines.init

      def parseStanzas(i: Int, acc: List[Stanza]): Either[String, (List[Stanza], Int)] =
        if i >= content.length then Left("header ended without MAC line")
        else if content(i).startsWith("-> ") then
          val args = content(i).drop(3).split(" ", -1).toList
          if args.exists(a => a.isEmpty || !a.forall(isArgChar)) then Left(s"invalid stanza arguments: ${content(i)}")
          else
            parseBody(i + 1, Vector.empty).flatMap { (bodyText, next) =>
              Primitives.base64Decode(bodyText).leftMap(e => s"invalid stanza body: $e").flatMap { body =>
                parseStanzas(next, Stanza(args, body) :: acc)
              }
            }
        else Right((acc.reverse, i))

      def parseBody(i: Int, acc: Vector[String]): Either[String, (String, Int)] =
        if i >= content.length then Left("unterminated stanza body")
        else
          val line = content(i)
          if line.length > BodyColumns || !line.forall(isBase64Char) then Left("invalid stanza body line")
          else if line.length < BodyColumns then Right(((acc :+ line).mkString, i + 1))
          else parseBody(i + 1, acc :+ line)

      parseStanzas(1, Nil).flatMap { (stanzas, i) =>
        if stanzas.isEmpty then Left("header has no stanzas")
        else if i != content.length - 1 then Left("unexpected line in header")
        else
          val macLine = content(i)
          if !macLine.startsWith("--- ") then Left("invalid MAC line")
          else
            Primitives.base64Decode(macLine.drop(4)).leftMap(e => s"invalid MAC: $e").flatMap { mac =>
              if mac.length != 32 then Left("invalid MAC length")
              else
                val macInputLength = raw.length - (macLine.length + 1) + 3
                Right(Header(stanzas, raw.take(macInputLength), mac))
            }
      }
