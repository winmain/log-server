package core.storage

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util

import com.fasterxml.jackson.annotation.JsonValue
import utils.MurmurHash3

import scala.util.Try

object Storage {
  val Charset = StandardCharsets.UTF_8

  /**
    * Максимальный размер буфера Array[Byte], созданный специально для защиты от чтения испроченных
    * данных, чтобы избежать ошибки OutOfMemoryError.
    * Это должно быть такое значение, чтобы любая лог-запись уместилась в этот буфер, но не настолько
    * большим, чтобы вызывать OutOfMemoryError в случае ошибочного чтения.
    */
  val MaxBytesBuffer = 1024 * 1024
  //  val MagicEnding = 0xe0ffe0ff

  sealed trait RecordId {
    def hash: Int
    def length: Int

    @JsonValue
    def jsonValue: Any =
      this match {
        case i: IntRecordId    => i.value
        case s: StringRecordId => new String(s.value, Storage.Charset)
        case EmptyRecordId     => null
      }

    override def equals(other: Any): Boolean = {
      this match {
        case i: IntRecordId =>
          other.isInstanceOf[IntRecordId] && other
            .asInstanceOf[IntRecordId]
            .value == i.value
        case s: StringRecordId =>
          other.isInstanceOf[StringRecordId] && util.Arrays
            .equals(other.asInstanceOf[StringRecordId].value, s.value)
        case EmptyRecordId =>
          this == EmptyRecordId
      }
    }
  }

  object RecordId {
    val EmptyIdMarker: Byte = 0
    val IntIdMarker: Byte = 1
    val StringRecordMarker: Byte = 2

    def apply(i: Int): RecordId = new IntRecordId(i)
    def apply(s: String): RecordId =
      new StringRecordId(s.getBytes(Storage.Charset))

    def str(bytes: Array[Byte]): RecordId =
      new StringRecordId(bytes)

    def empty: RecordId = EmptyRecordId

    def parse(s: String): RecordId =
      Try(s.toInt).map(RecordId(_)).getOrElse(RecordId(s))
  }

  class IntRecordId(val value: Int) extends RecordId {
    def hash: Int = value
    def length: Int = 5
    override def toString: String = value.toString
  }

  class StringRecordId(val value: Array[Byte]) extends RecordId {
    def hash: Int = value.foldLeft(0)(_ + _ * 31)
    def length: Int = 1 + 4 + value.length
    override def toString: String = new String(value, Storage.Charset)
  }

  case object EmptyRecordId extends RecordId {
    override def hash: Int = 0
    override def length: Int = 1
  }

  case class Header(timestamp: Long,
                    offset: Int,
                    hash: Int,
                    tableName: String,
                    id: RecordId)
  case class Record(timestamp: Long,
                    tableName: String,
                    id: RecordId,
                    data: Array[Byte]) {
    def calcHash: Int = Storage.calcHash(data, timestamp)
    override def equals(obj: scala.Any): Boolean = obj match {
      case r: Record =>
        (this eq r) || (timestamp == r.timestamp && tableName == r.tableName && id == r.id && util.Arrays
          .equals(data, r.data))
      case _ => false
    }
    def makeHeader(offset: Int): Header =
      Header(
        timestamp = timestamp,
        offset = offset,
        hash = calcHash,
        tableName = tableName,
        id = id
      )
  }

  def calcHash(data: Array[Byte], timestamp: Long): Int =
    MurmurHash3.hash(data) ^ ((timestamp >> 32) ^ (timestamp & 0xffffffff)).toInt

  private[core] def readBytes(read: ReadStream): Array[Byte] = {
    val size: Int = read.getInt
    if (size > MaxBytesBuffer)
      throw new IOException(
        "Read too big byte array size: " + size + ". Broken data?"
      )
    val bytes = new Array[Byte](size)
    read.get(bytes)
    bytes
  }
  private[core] def writeBytes(rw: ReadWrite, bytes: Array[Byte]): Unit = {
    require(
      bytes.length <= MaxBytesBuffer,
      "Cannot write too big byte array of size: " + bytes.length + ", max: " + MaxBytesBuffer
    )
    rw.putInt(bytes.length)
    rw.put(bytes)
  }

  private[core] def readLongByteBuffer(read: ReadStream,
                                       count: Int): LongByteBuffer = {
    val lbb: LongByteBuffer = new LongByteBuffer(count)
    read.get(lbb.bb)
    lbb
  }
  private[core] def writeLongByteBuffer(rw: ReadWrite, lbb: LongByteBuffer) {
    lbb.bb.rewind()
    rw.put(lbb.bb)
  }
}
