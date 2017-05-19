package utils
import java.nio.ByteBuffer

import org.specs2.DataTables2
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

class BufferUtilsSpec extends Specification with DataTables2 {
  "Test putInt, getInt" in {
    case class Row(v: Int, bytes: Iterable[Int]) extends CheckedRow {
      override def validate: MatchResult[Any] = {
        val buf = ByteBuffer.allocate(5)
        BufferUtils.putInt(buf, v)
        val pos = buf.position()
        val result1: Seq[Byte] = buf.array().take(pos).toSeq
        buf.flip()
        val value2: Int = BufferUtils.getInt(buf)
        result1 === bytes.map(_.toByte) and v === value2 and buf.position() === pos
      }
    }
    table1(
      Row(0, Seq(0)),
      Row(1, Seq(1)),
      Row(0x67, Seq(0x67)),
      Row(0x7f, Seq(0x7f)),
      Row(0x80, Seq(0x80, 0x80)),
      Row(0x81, Seq(0x80, 0x81)),
      Row(0x2e54, Seq(0xae, 0x54)),
      Row(0x3fff, Seq(0xbf, 0xff)),
      Row(0x4000, Seq(0xc0, 0x40, 0x00)),
      Row(0x4001, Seq(0xc0, 0x40, 0x01)),
      Row(0x1c9052, Seq(0xdc, 0x90, 0x52)),
      Row(0x1fffff, Seq(0xdf, 0xff, 0xff)),
      Row(0x200000, Seq(0xe0, 0x20, 0x00, 0x00)),
      Row(0x200001, Seq(0xe0, 0x20, 0x00, 0x01)),
      Row(0x49a450, Seq(0xe0, 0x49, 0xa4, 0x50)),
      Row(0x0e123456, Seq(0xee, 0x12, 0x34, 0x56)),
      Row(0x10000000, Seq(0xff, 0x10, 0x00, 0x00, 0x00)),
      Row(0x589ae453, Seq(0xff, 0x58, 0x9a, 0xe4, 0x53)),
      Row(0xcdef0123, Seq(0xff, 0xcd, 0xef, 0x01, 0x23)),
      Row(-1, Seq(0xff, 0xff, 0xff, 0xff, 0xff))
    )
  }

  "Test putString, getString" in {
    case class Row(s: String, byteLength: Int) extends CheckedRow {
      override def validate: MatchResult[Any] = {
        val buf = ByteBuffer.allocate(16)
        BufferUtils.putString(buf, s)
        val pos = buf.position()
        buf.flip()
        val s2: String = BufferUtils.getString(buf)
        s === s2 and pos === byteLength and buf.position() === pos
      }
    }
    table1(
      Row("", 1),
      Row("0", 2),
      Row("abc", 4),
      Row("буквы", 11),
      Row("йй bb", 8)
    )
  }
}
