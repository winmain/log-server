package command
import java.nio.file.{Files, Path, Paths}

import core.reader.{MemoryWiseLogWrapper, NewLogReader}
import core.storage.{AppendableBigStorage, RealDirectory}
import org.slf4j.Logger

case class UpdateCommand() extends Command {
  /**
   * Добавить новые данные в базу и удалить успешно прочитанные данные логов.
   * Например: dbDir = /mnt/test/logs/2015, updates = /home/myproject/log/sql-new
   * dbDir - Путь до базы данных логов
   * updates - Обновления - каталог с обновлениями, либо сами файлы обновлений
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length < 2) exitError("Usage: convert-month <db-dir> <updates>")

    val dbDir = Paths.get(params(0))
    val updatePaths: Vector[Path] = params.drop(1).map(Paths.get(_))(collection.breakOut)

    val big: AppendableBigStorage = new AppendableBigStorage(new RealDirectory(dbDir))
    val logReader: NewLogReader = new NewLogReader(updatePaths, log)
    try {
      new MemoryWiseLogWrapper(logReader).addRecords(big, log)
    } finally {
      log.info("Closing BigStorage")
      big.close()

      log.info("Removing processed update files")
      logReader.processedSources.foreach(Files.delete)
    }

    log.info("Finished update")
  }
}
