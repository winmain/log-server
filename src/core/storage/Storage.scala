package core.storage

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, LongBuffer}
import java.util

import utils.MurmurHash3

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

  case class Header(timestamp: Long, offset: Int, hash: Int, tableName: String, id: Int)
  case class Record(timestamp: Long, tableName: String, id: Int, data: Array[Byte]) {
    def calcHash: Int = Storage.calcHash(data, timestamp)
    override def equals(obj: scala.Any): Boolean = obj match {
      case r: Record => (this eq r) || (timestamp == r.timestamp && tableName == r.tableName && id == r.id && util.Arrays.equals(data, r.data))
      case _ => false
    }
    def makeHeader(offset: Int): Header =
      Header(timestamp = timestamp, offset = offset, hash = calcHash, tableName = tableName, id = id)
  }

  def calcHash(data: Array[Byte], timestamp: Long): Int = MurmurHash3.hash(data) ^ ((timestamp >> 32) ^ (timestamp & 0xffffffff)).toInt


  private[core] def readBytes(read: ReadStream): Array[Byte] = {
    val size: Int = read.getInt
    if (size > MaxBytesBuffer) throw new IOException("Read too big byte array size: " + size + ". Broken data?")
    val bytes = new Array[Byte](size)
    read.get(bytes)
    bytes
  }
  private[core] def writeBytes(rw: ReadWrite, bytes: Array[Byte]): Unit = {
    require(bytes.length <= MaxBytesBuffer, "Cannot write too big byte array of size: " + bytes.length + ", max: " + MaxBytesBuffer)
    rw.putInt(bytes.length)
    rw.put(bytes)
  }

  private[core] def readLongByteBuffer(read: ReadStream, count: Int): LongByteBuffer = {
    val lbb: LongByteBuffer = new LongByteBuffer(count)
    read.get(lbb.bb)
    lbb
  }
  private[core] def writeLongByteBuffer(rw: ReadWrite, lbb: LongByteBuffer) {
    lbb.bb.rewind()
    rw.put(lbb.bb)
  }
}
