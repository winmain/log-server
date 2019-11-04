package com.github.winmain.logserver.client

import java.io.DataOutputStream
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.github.winmain.logserver.core.RecordId.{EmptyRecordId, IntRecordId, StringRecordId}
import com.github.winmain.logserver.core.UInt29Writer.toUInt29WriterOps
import com.github.winmain.logserver.core._
import org.slf4j.Logger

import scala.annotation.tailrec

/**
 * Клиент записи логов
 *
 * @param writeDir           Каталог, где будут логи
 * @param savedFileFormat    Формат сохранённых файлов
 * @param fileLifetimeMillis Время жизни "current" файла в мс
 * @param addProcessSuffix   Добавлять к имени файла "current" id процесса? Актуально для крон-заданий.
 */
class LogWriterClient(writeDir: Path,
                      savedFileFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'{suffix}.saved'"),
                      fileLifetimeMillis: Long = 5 * 60 * 1000L,
                      addProcessSuffix: Boolean = false,
                      logger: Logger) {

  import LogWriterClient._

  private val lock = new Object
  private var fileStream: DataOutputStream = _
  private var fileEndWrite: Long = 0L

  private val currentFile = writeDir.resolve(
    if (addProcessSuffix) "current-" + ManagementFactory.getRuntimeMXBean.getName.split('@')(0)
    else "current")

  def append(normalizedTableName: String, timestamp: Long, log: String): Unit =
    append(normalizedTableName, RecordId.empty, timestamp, log)

  def append(normalizedTableName: String, id: Int, timestamp: Long, log: String): Unit =
    append(normalizedTableName, RecordId(id), timestamp, log)

  def append(normalizedTableName: String, id: String, timestamp: Long, log: String): Unit =
    append(normalizedTableName, RecordId(id), timestamp, log)

  def close(): Unit = {
    if (fileStream != null) {
      fileStream.close()
      renameFileToSaved(currentFile)
    }
    fileStream = null
  }

  // internal

  private def append(normalizedTableName: String, id: RecordId, timestamp: Long, log: String): Unit = {
    def writeStr(s: String): Unit = {
      val bytes = s.getBytes(LogServer.Charset)
      fileStream.writeUInt29(bytes.length)
      fileStream.write(bytes)
    }

    lock.synchronized {
      val time = System.currentTimeMillis()
      // file autorotate
      if (time >= fileEndWrite) {
        fileEndWrite = time + fileLifetimeMillis
        close()
        autoRenameOldUnclosedFiles(time)
        val file: Path = currentFile
        Files.createDirectories(file.getParent)
        if (!Files.exists(file)) Files.createFile(file)
        fileStream = new DataOutputStream(Files.newOutputStream(file, StandardOpenOption.APPEND))
        fileStream.writeInt(LogServer.StorageVersion)
      }

      // write data
      writeStr(normalizedTableName)
      writeId(id)
      fileStream.writeLong(timestamp)
      writeStr(log)
    }
  }

  private def writeId(id: RecordId): Unit =
    id match {
      case id: IntRecordId =>
        fileStream.writeByte(RecordId.IntIdMarker)
        fileStream.writeInt(id.value)

      case id: StringRecordId =>
        fileStream.writeByte(RecordId.StringIdMarker)
        fileStream.writeUInt29(id.value.length)
        fileStream.write(id.value)

      case EmptyRecordId =>
        fileStream.writeByte(RecordId.EmptyIdMarker)
    }

  /**
   * Переименовать текущий файл логов в сохранённый. Учитывается коллизия имён, и подбирается
   * имя файла, который не существует на данный момент.
   *
   * @param path Файл, который следует переименовать.
   */
  private def renameFileToSaved(path: Path): Unit = {
    val dateTime: LocalDateTime = LocalDateTime.now()

    @tailrec def findFileToSave(suffixIdx: Int): Path = {
      var name: String = savedFileFormat.format(dateTime)
      name = name.replace("{suffix}", if (suffixIdx > 0) "-" + suffixIdx else "")
      val file = writeDir.resolve(name)
      if (Files.exists(file)) findFileToSave(suffixIdx + 1)
      else file
    }

    Files.move(path, findFileToSave(0))
  }

  /**
   * Автоматически переименовать все current-файлы в каталоге в saved.
   * Это действие надо выполнять для случаев внезапной остановки сервера, чтобы лог-файлы не оставались
   * не сохранёнными.
   */
  private def autoRenameOldUnclosedFiles(now: Long): Unit = {
    if (Files.exists(writeDir) && Files.isDirectory(writeDir)) {
      Files.list(writeDir)
        .filter(_.getFileName.toString.startsWith("current"))
        .forEach { path =>
          logger.warn("Saving lost log file: " + path.toString)
          renameFileToSaved(path)
        }
    }
  }
}

object LogWriterClient {

  implicit val dataOutputStreamUInt29Writer: UInt29Writer[DataOutputStream] = _.writeByte(_)

}