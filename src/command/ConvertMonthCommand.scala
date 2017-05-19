package command

import java.nio.file.Paths

import core.reader.{MemoryWiseLogWrapper, OldLogReader}
import core.storage.{AppendableBigStorage, RealDirectory}
import org.slf4j.Logger

case class ConvertMonthCommand() extends Command {

  /**
   * Сконвертировать месяц.
   * Например: dbDir = /mnt/test/logs/2015, monthDir = /home/myproject/log/sql/15/04
   * dbDir - Путь до базы данных логов
   * monthDir - Путь до исходных логов для указанного месяца
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length != 2) exitError("Usage: convert-month <db-dir> <old-month-dir>")

    val dbDir = Paths.get(params(0))
    val monthDir = Paths.get(params(1))

    log.info("Converting month: " + monthDir)
    val big: AppendableBigStorage = new AppendableBigStorage(new RealDirectory(dbDir))
    new MemoryWiseLogWrapper(new OldLogReader(monthDir.toFile)).addRecords(big, log)
    log.info("Closing BigStorage")
    big.close()

    log.info("Finished converting month")
  }
}
