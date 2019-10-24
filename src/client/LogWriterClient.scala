package client
import java.io.{DataOutputStream, File, FileOutputStream, FilenameFilter}
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import core.storage.Storage.RecordId
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/**
  * Клиент записи логов
  * @param writeDir Каталог, где будут логи
  * @param savedFileFormat Формат сохранённых файлов
  * @param fileLifetimeMillis Время жизни "current" файла в мс
  * @param addProcessSuffix Добавлять к имени файла "current" id процесса? Актуально для крон-заданий.
  */
class LogWriterClient(writeDir: File,
                      savedFileFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'{suffix}.saved'"),
                      fileLifetimeMillis: Long = 5 * 60 * 1000L,
                      addProcessSuffix: Boolean = false) {
  private val lock = new Object
  private var fileStream: DataOutputStream = null
  private var fileEndWrite: Long = 0L

  private val currentFile = new File(writeDir,
    if (addProcessSuffix) "current-" + ManagementFactory.getRuntimeMXBean.getName.split('@')(0)
    else "current")

  val version = 2
  val charset = StandardCharsets.UTF_8

  def append(normalizedTableName: String, maybeId: Option[Int], timestamp: Long, log: String) {
    lock.synchronized {
      val time = System.currentTimeMillis()
      // file autorotate
      if (time >= fileEndWrite) {
        fileEndWrite = time + fileLifetimeMillis
        close()
        autoRenameOldUnclosedFiles(time)
        val file: File = currentFile
        file.getParentFile.mkdirs()
        fileStream = new DataOutputStream(new FileOutputStream(file))
        fileStream.writeInt(version)
      }

      // write data
      writeStr(normalizedTableName)
      maybeId match {
        case None =>
          fileStream.writeByte(RecordId.EmptyIdMarker)
        case Some(id) =>
          fileStream.writeByte(RecordId.IntIdMarker)
          fileStream.writeInt(id)
      }
      fileStream.writeLong(timestamp)
      writeStr(log)

      def writeStr(s: String): Unit = {
        val bytes = s.getBytes(charset)
        fileStream.writeInt(bytes.length)
        fileStream.write(bytes)
      }
    }
  }

  def close(): Unit = {
    if (fileStream != null) {
      fileStream.close()
      renameFileToSaved(currentFile)
    }
    fileStream = null
  }

  /**
    * Переименовать текущий файл логов в сохранённый. Учитывается коллизия имён, и подбирается
    * имя файла, который не существует на данный момент.
    * @param file Файл, который следует переименовать.
    */
  private def renameFileToSaved(file: File): Unit = {
    val dateTime: LocalDateTime = LocalDateTime.now()
    @tailrec def findFileToSave(suffixIdx: Int): File = {
      var name: String = savedFileFormat.format(dateTime)
      name = name.replace("{suffix}", if (suffixIdx > 0) "-" + suffixIdx else "")
      val file = new File(writeDir, name)
      if (file.exists()) findFileToSave(suffixIdx + 1)
      else file
    }
    file.renameTo(findFileToSave(0))
  }

  /**
    * Автоматически переименовать все current-файлы в каталоге в saved,
    * если они не записывались более 24 часов.
    * Это действие надо выполнять для случаев внезапной остановки сервера, чтобы лог-файлы не оставались
    * не сохранёнными.
    */
  private def autoRenameOldUnclosedFiles(now: Long): Unit = {
    val maxTime = now - TimeUnit.DAYS.toMillis(1)
    if (writeDir.exists() && writeDir.isDirectory) {
      writeDir.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.startsWith("current-")
      }).withFilter(_.lastModified() < maxTime)
        .foreach {file =>
          LoggerFactory.getLogger(getClass).warn("Saving lost log file: " + file.toString)
          renameFileToSaved(file)
        }
    }
  }
}
