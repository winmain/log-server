package com.github.winmain.logserver.db.reader
import java.io._
import java.nio.channels.{Channels, FileChannel}
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.BlockingQueue
import java.util.zip.GZIPInputStream

import com.github.winmain.logserver.core.UInt29Reader._
import com.github.winmain.logserver.core.{LogServer, RecordId, UInt29Reader}
import com.github.winmain.logserver.db.SourceLogRecord
import com.github.winmain.logserver.db.storage.Storage
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class NewLogReader(sources: Seq[Path], log: Logger) extends LogReader {
  import NewLogReader._

  val charset: Charset = StandardCharsets.UTF_8

  val processedSources: mutable.Buffer[Path] = new ArrayBuffer[Path]()

  class Record(val tableName: String,
               val id: RecordId,
               val timestamp: Long,
               val logBytesUTF8: Array[Byte])
      extends SourceLogRecord {
    override def toString: String =
      "Record(" + tableName + "," + id + ",logBytes:" + logBytesUTF8.length + ")"
  }

  override def readLogs(result: BlockingQueue[SourceLogRecord]): Unit =
    sources.foreach(doReadLogs(_, result))

  private def doReadLogs(source: Path,
                         result: BlockingQueue[SourceLogRecord]): Unit = {
    source match {
      case dir if Files.isDirectory(dir) =>
        Files
          .newDirectoryStream(dir)
          .asScala
          .toVector
          .sorted
          .foreach(doReadLogs(_, result))
      case path =>
        val name: String = path.getFileName.toString
        if (name.endsWith(".saved") || name.endsWith(".saved.gz"))
          doReadLogFromFile(path, result)
    }
  }

  private def doReadLogFromFile(
    logPath: Path,
    result: BlockingQueue[SourceLogRecord]
  ): Unit = {
    var fileChannel: FileChannel = null
    try {
      val logFileName: String = logPath.getFileName.toString

      log.info("Reading " + logPath)
      fileChannel = FileChannel.open(logPath, StandardOpenOption.READ)

      val stream = new DataInputStream({
        val bufStream: BufferedInputStream =
          new BufferedInputStream(Channels.newInputStream(fileChannel))
        if (logFileName.endsWith(".gz")) new GZIPInputStream(bufStream)
        else bufStream
      })
      val version = stream.readInt()
      require(version == LogServer.StorageVersion, "Invalid version: " + version)

      while (stream.available() > 0) {
        val tableName = readStr().intern()
        require(tableName.nonEmpty, "Empty tableName in file " + logPath)
        require(
          tableName != "\u0000",
          "Invalid read tableName. Version bytes in middle of file? " + logPath
        )
        val id: RecordId = stream.readRecordId()
        val timestamp: Long = stream.readLong()
        val log = readStr()
        require(log.nonEmpty, "Empty log in file " + logPath)
        result.put(
          new Record(
            tableName,
            id,
            timestamp,
            log.getBytes(StandardCharsets.UTF_8)
          )
        )
      }

      def readStr(): String = {
        val length: Int = stream.readUInt29()
        if (length > Storage.MaxBytesBuffer)
          throw new IOException(
            "Read too big byte array size: " + length + ". Broken data?"
          )
        val bytes = new Array[Byte](length)
        stream.read(bytes)
        new String(bytes, charset)
      }

      stream.close()
      processedSources += logPath
    } catch {
      case _: InterruptedException => // just return
      case e: Throwable =>
        throw new RuntimeException(
          "Error reading " + logPath + " at " + (if (fileChannel == null) "[unknown]"
                                                 else fileChannel.position()),
          e
        )
    }
  }
}

object NewLogReader {

  implicit val dataInputStreamUInt29Reader: UInt29Reader[DataInputStream] = _.readByte()

  implicit class DataInputStreamOps(val in: DataInputStream) extends AnyVal {

    def readRecordId(): RecordId = {
      in.readByte() match {
        case RecordId.StringIdMarker =>
          val size = in.readUInt29()
          val bytes = new Array[Byte](size)
          in.read(bytes)

          RecordId.str(bytes)

        case RecordId.EmptyIdMarker =>
          RecordId.empty

        case RecordId.IntIdMarker =>
          RecordId(in.readInt())
      }

    }

  }

}
