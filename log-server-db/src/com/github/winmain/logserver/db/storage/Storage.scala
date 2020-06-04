package com.github.winmain.logserver.db.storage

import java.io.IOException
import java.util

import com.github.winmain.logserver.core.RecordId
import com.github.winmain.logserver.core.UInt29Reader._
import com.github.winmain.logserver.core.UInt29Writer._
import com.github.winmain.logserver.db.utils.MurmurHash3

object Storage {

  /**
    * Максимальный размер буфера Array[Byte], созданный специально для защиты от чтения испроченных
    * данных, чтобы избежать ошибки OutOfMemoryError.
    * Это должно быть такое значение, чтобы любая лог-запись уместилась в этот буфер, но не настолько
    * большим, чтобы вызывать OutOfMemoryError в случае ошибочного чтения.
    */
  val MaxBytesBuffer: Int = 1024 * 1024
  //  val MagicEnding = 0xe0ffe0ff

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

  private[db] def readBytes(read: ReadStream): Array[Byte] = {
    val size: Int = read.readUInt29()
    if (size > MaxBytesBuffer)
      throw new IOException(
        "Read too big byte array size: " + size + ". Broken data?"
      )
    val bytes = new Array[Byte](size)
    read.get(bytes)
    bytes
  }
  private[db] def writeBytes(rw: ReadWrite, bytes: Array[Byte]): Unit = {
    require(
      bytes.length <= MaxBytesBuffer,
      "Cannot write too big byte array of size: " + bytes.length + ", max: " + MaxBytesBuffer
    )
    rw.writeUInt29(bytes.length)
    rw.put(bytes)
  }

  private[db] def readLongByteBuffer(read: ReadStream, count: Int): LongByteBuffer = {
    val lbb: LongByteBuffer = new LongByteBuffer(count)
    read.get(lbb.bb)
    lbb
  }

  private[db] def writeLongByteBuffer(rw: ReadWrite, lbb: LongByteBuffer): Unit = {
    lbb.bb.rewind()
    rw.put(lbb.bb)
  }
}
