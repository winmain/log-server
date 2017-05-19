package core.reader
import java.io._
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.BlockingQueue
import java.util.zip.GZIPInputStream

import core.SourceLogRecord
import core.storage.Storage
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Прочитать логи в новом формате, версия 1.
 */
class NewLogReader(sources: Seq[Path], log: Logger) extends LogReader {
  val charset = StandardCharsets.UTF_8

  val processedSources: mutable.Buffer[Path] = new ArrayBuffer[Path]()

  class Record(val tableName: String, val id: Option[Int], val timestamp: Long, val logBytesUTF8: Array[Byte]) extends SourceLogRecord {
    override def toString: String = "Record(" + tableName + "," + id + ",logBytes:" + logBytesUTF8.length + ")"
  }

  override def readLogs(result: BlockingQueue[SourceLogRecord]): Unit = sources.foreach(doReadLogs(_, result))

  private def doReadLogs(source: Path, result: BlockingQueue[SourceLogRecord]): Unit = {
    source match {
      case dir if Files.isDirectory(dir) =>
        Files.newDirectoryStream(dir).toVector.sorted.foreach(doReadLogs(_, result))
      case path =>
        val name: String = path.getFileName.toString
        if (name.endsWith(".saved") || name.endsWith(".saved.gz")) doReadLogFromFile(path, result)
    }
  }

  private def doReadLogFromFile(logPath: Path, result: BlockingQueue[SourceLogRecord]): Unit = {
    var raf: RandomAccessFile = null
    try {
      val logFileName: String = logPath.getFileName.toString

      log.info("Reading " + logPath)
      raf = new RandomAccessFile(logPath.toFile, "r")
      val stream = new DataInputStream({
        val bufStream: BufferedInputStream = new BufferedInputStream(Channels.newInputStream(raf.getChannel))
        if (logFileName.endsWith(".gz")) new GZIPInputStream(bufStream)
        else bufStream
      })
      val version = stream.readInt()
      require(version == 1, "Invalid version: " + version)

      while (stream.available() > 0) {
        val tableName = readStr().intern()
        require(tableName.nonEmpty, "Empty tableName in file " + logPath)
        require(tableName != "\u0000", "Invalid read tableName. Version bytes in middle of file? " + logPath)
        val maybeId: Option[Int] =
          stream.read() match {
            case 0 => None
            case 1 => Some(stream.readInt())
          }
        val timestamp: Long = stream.readLong()
        val log = readStr()
        require(log.nonEmpty, "Empty log in file " + logPath)
        result.put(new Record(tableName, maybeId, timestamp, log.getBytes(StandardCharsets.UTF_8)))
      }

      def readStr(): String = {
        val length: Int = stream.readInt()
        if (length > Storage.MaxBytesBuffer) throw new IOException("Read too big byte array size: " + length + ". Broken data?")
        val bytes = new Array[Byte](length)
        stream.read(bytes)
        new String(bytes, charset)
      }

      stream.close()
      processedSources += logPath
    } catch {
      case _: InterruptedException => // just return
      case e: Throwable =>
        throw new RuntimeException("Error reading " + logPath + " at " + (if (raf == null) "[unknown]" else raf.getFilePointer), e)
    }
  }
}
