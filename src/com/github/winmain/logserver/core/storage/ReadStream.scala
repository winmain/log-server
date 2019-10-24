package com.github.winmain.logserver.core.storage
import java.io._
import java.nio.file.{Files, Path}
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util.zip.GZIPInputStream

import com.github.winmain.logserver.core.RecordId
import com.github.winmain.logserver.core.RecordId.{EmptyRecordId, IntRecordId, StringRecordId}

// ------------------------------- ReadStream -------------------------------

trait ReadStream {
  def pos: Long
  def skip(n: Long)
  def available: Boolean
  def close()

  /** Вернуть размер всего файла. Если размер недоступен, вернётся None (для gzip файлов). */
  def maybeLength: Option[Long]

  def getByte: Byte
  def getShort: Short
  def getInt: Int
  def getLong: Long
  def get(dst: Array[Byte])
  def get(dst: ByteBuffer)
}

object ReadStream {

  implicit class Ops(val readStream: ReadStream) extends AnyVal {
    def getRecordId: RecordId =
      readStream.getByte match {
        case RecordId.StringIdMarker =>
          val size = readStream.getInt
          val bytes = new Array[Byte](size)
          readStream.get(bytes)

          RecordId.str(bytes)

        case RecordId.EmptyIdMarker =>
          RecordId.empty

        case RecordId.IntIdMarker =>
          RecordId(readStream.getInt)
      }
  }

}

class ReadDataStream(s: DataInputStream, val maybeLength: Option[Long])
    extends ReadStream {
  private var p = 0L
  override def pos: Long = p
  override def available: Boolean = s.available() > 0
  override def skip(n: Long) = {
    var toSkip = n
    while (toSkip > 0) {
      toSkip -= s.skip(toSkip)
    }
    p += n
  }
  override def close(): Unit = s.close()

  override def getByte: Byte = { p += 1; s.readByte() }
  override def getShort: Short = { p += 2; s.readShort() }
  override def getInt: Int = { p += 4; s.readInt() }
  override def getLong: Long = { p += 8; s.readLong() }
  override def get(dst: Array[Byte]): Unit = {
    p += dst.length; s.readFully(dst)
  }
  override def get(dst: ByteBuffer): Unit = {
    p += dst.remaining();
    s.readFully(
      dst.array(),
      dst.arrayOffset() + dst.position(),
      dst.remaining()
    )
  }
}

object EmptyDataStream extends ReadStream {
  override def pos: Long = 0L
  override def available: Boolean = false
  override def skip(n: Long) = sys.error("Stream is empty")
  override def close(): Unit = {}
  override def maybeLength: Option[Long] = Some(0L)

  override def getByte: Byte = sys.error("Stream is empty")
  override def getShort: Short = sys.error("Stream is empty")
  override def getInt: Int = sys.error("Stream is empty")
  override def getLong: Long = sys.error("Stream is empty")
  override def get(dst: Array[Byte]): Unit = sys.error("Stream is empty")
  override def get(dst: ByteBuffer): Unit = sys.error("Stream is empty")
}

object ReadDataStream {
  def fromPath(path: Path, bufferSize: Int): ReadStream = {
    if (Files.exists(path)) {
      val raf: RandomAccessFile = new RandomAccessFile(path.toFile, "r")
      if (path.getFileName.toString.endsWith(".gz"))
        new ReadDataStream(
          new DataInputStream(
            new GZIPInputStream(new FileInputStream(raf.getFD), bufferSize)
          ),
          None
        )
      else
        new ReadDataStream(
          new DataInputStream(
            new BufferedInputStream(new FileInputStream(raf.getFD), bufferSize)
          ),
          Some(raf.length())
        )
    } else EmptyDataStream
  }
}

/**
  * Matcher для исключений конца файла как для реального файла, так и для буфера
  */
object IoDataStreamException {
  def apply(t: Throwable): Boolean = t match {
    case _: IOException | _: BufferUnderflowException => true
    case _                                            => false
  }
  def unapply(t: Throwable): Option[Throwable] = if (apply(t)) Some(t) else None
}

// ------------------------------- ReadWrite -------------------------------

trait ReadWrite extends ReadStream {
  def seek(n: Long)
  def length: Long
  def filePath: String
  def truncate(n: Long)

  def putByte(v: Byte)
  def putShort(v: Short)
  def putInt(v: Int)
  def putLong(v: Long)
  def put(src: Array[Byte])
  def put(src: ByteBuffer)
}

object ReadWrite {

  implicit class Ops(val readWrite: ReadWrite) extends AnyVal {
    def putRecordId(recordId: RecordId): Unit =
      recordId match {
        case i: IntRecordId =>
          readWrite.putByte(RecordId.IntIdMarker)
          readWrite.putInt(i.value)

        case s: StringRecordId =>
          readWrite.putByte(RecordId.StringIdMarker)
          readWrite.putInt(s.value.length)
          readWrite.put(s.value)

        case EmptyRecordId =>
          readWrite.putByte(RecordId.EmptyIdMarker)
      }
  }

}

class ReadWriteFile(file: File, mode: String = "rw") extends ReadWrite {
  val raf: RandomAccessFile = new RandomAccessFile(file, mode)
  override def pos: Long = raf.getFilePointer
  override def skip(n: Long): Unit = raf.skipBytes(n.toInt)
  override def seek(n: Long): Unit = raf.seek(n)
  override def available: Boolean = raf.getFilePointer < raf.length()
  override def close(): Unit = raf.close()
  override def maybeLength: Option[Long] = Some(raf.length())
  override def length: Long = raf.length()
  override def filePath: String = file.getPath
  override def truncate(n: Long): Unit = raf.getChannel.truncate(n)

  override def getByte: Byte = raf.readByte()
  override def getShort: Short = raf.readShort()
  override def getInt: Int = raf.readInt()
  override def getLong: Long = raf.readLong()
  override def get(dst: Array[Byte]): Unit = raf.readFully(dst)
  override def get(dst: ByteBuffer): Unit = raf.getChannel.read(dst)

  override def putByte(v: Byte): Unit = raf.writeByte(v)
  override def putShort(v: Short): Unit = raf.writeShort(v)
  override def putInt(v: Int): Unit = raf.writeInt(v)
  override def putLong(v: Long): Unit = raf.writeLong(v)
  override def put(src: Array[Byte]): Unit = raf.write(src)
  override def put(src: ByteBuffer): Unit = raf.getChannel.write(src)
}

// TODO: при реализации ReadWriteFile в gzip-формате, при открытии этого файла на запись
// сначала следует создать новый temporary файл, в который нужно распаковать старый файл,
// а потом после работы с ним, при закрытии запаковать его обратно в gzip, и сохранить.
// Также, нужно учесть, что после открытия ReadWrite, если открыть ReadStream, то он должен уже
// открыть не gzip файл, а новосозданную копию.

/**
  * Эмуляция чтения/записи в файл используя буфер #buf.
  *
  * @param buf Буфер для чтения и записи. [[buf.limit()]] задаёт виртуальный размер файла,
  *            который может увеличиваться при записи в него.
  * @param emptyBuffer Создать пустой буфер? У такого буфера лимит сброшен на 0?
  */
class ReadWriteBuffer(buf: ByteBuffer, emptyBuffer: Boolean = false)
    extends ReadWrite {
  buf.rewind()
  if (emptyBuffer) buf.limit(0)
  override def pos: Long = buf.position()
  override def skip(n: Long): Unit = {
    require(n >= 0L); buf.position(buf.position() + n.toInt)
  }
  override def seek(n: Long): Unit = buf.position(n.toInt)
  override def available: Boolean = buf.hasRemaining
  override def close(): Unit = {}
  override def maybeLength: Option[Long] = Some(buf.limit().toLong)
  override def length: Long = buf.limit()
  override def filePath: String = "ByteBuffer"
  override def truncate(n: Long): Unit = buf.limit(n.toInt)

  override def getByte: Byte = buf.get
  override def getShort: Short = buf.getShort
  override def getInt: Int = buf.getInt
  override def getLong: Long = buf.getLong
  override def get(dst: Array[Byte]): Unit = buf.get(dst)
  override def get(dst: ByteBuffer): Unit = dst.put(buf)

  private def updateLimit(writeBytes: Int): ByteBuffer = {
    val newLimit = pos + writeBytes
    if (newLimit > buf.limit()) buf.limit(newLimit.toInt)
    buf
  }
  override def putByte(v: Byte): Unit = updateLimit(1).put(v)
  override def putShort(v: Short): Unit = updateLimit(2).putShort(v)
  override def putInt(v: Int): Unit = updateLimit(4).putInt(v)
  override def putLong(v: Long): Unit = updateLimit(8).putLong(v)
  override def put(src: Array[Byte]): Unit = updateLimit(src.length).put(src)
  override def put(src: ByteBuffer): Unit =
    updateLimit(src.remaining()).put(src)
}
