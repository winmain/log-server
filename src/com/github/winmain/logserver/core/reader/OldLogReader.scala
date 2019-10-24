package com.github.winmain.logserver.core.reader

import java.io._
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.BlockingQueue
import java.util.zip.GZIPInputStream

import com.github.winmain.logserver.core.storage.Storage.RecordId
import com.github.winmain.logserver.core.{SourceLogRecord, SourceUtils}
import com.github.winmain.logserver.utils.Dates.dateWrapper
import com.github.winmain.logserver.utils.InputStreamSplitter

/**
 * Reader для логов старого формата, когда на одну таблицу за один день создавалось по файлу.
 * Эти логи записаны в смешанной кодировке - и в utf-8, и в cp1251.
 */
class OldLogReader(baseDir: File) extends LogReader {

  private val files: Vector[File] = baseDir.listFiles().toVector.filter(_.isFile).sortBy(_.getPath)

  class Record(val tableName: String, log: String, logFile: File) extends SourceLogRecord {
    override val logBytesUTF8: Array[Byte] = log.getBytes(StandardCharsets.UTF_8)

    val id: RecordId = {
      val nlIdx = log.indexOf('\n')
      require(nlIdx != -1, "Error log in " + logFile + "\nLog: " + log)
      val id = OldLogReader.idExtractor.findFirstMatchIn(log.substring(0, nlIdx)).map(_.group(1).toInt)
      id.map(RecordId(_)).getOrElse(RecordId.empty)
    }

    val dateTime: LocalDateTime = LocalDateTime.parse(log.substring(5, 24), OldLogReader.dateFormat)
    override def timestamp: Long = dateTime.toMillis
  }

  def readLogs(result: BlockingQueue[SourceLogRecord]) {
    for {logFile <- files if logFile.length() > 0L
         tableNameMatch <- OldLogReader.tableNameExtractor.findFirstMatchIn(logFile.getName)
         tableName = tableNameMatch.group(1)} {
      try {
        val stream: FilterInputStream = {
          val bufStream: BufferedInputStream = new BufferedInputStream(new FileInputStream(logFile))
          if (logFile.getName.endsWith(".gz")) new GZIPInputStream(bufStream)
          else bufStream
        }
        val splitter = new InputStreamSplitter(stream, "\n\n---- ".getBytes, 4096)

        while (splitter.readNext()) {
          val log = SourceUtils.toStringDetectEncoding(splitter.getBuf, splitter.getStart, splitter.getLength).trim
          if (log.indexOf('\n') != -1) {
            result.put(new Record(tableName, log, logFile))
          }
        }
        stream.close()
      } catch {
        case e: Exception => throw new IOException("Error in file " + logFile, e)
      }
    }
  }
}

object OldLogReader {
  val tableNameExtractor = """^(.*)\.sql\.""".r
  val idExtractor = """ id:(\d+)""".r
  val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
}