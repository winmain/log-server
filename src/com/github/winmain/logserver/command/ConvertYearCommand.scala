package com.github.winmain.logserver.command

import java.nio.file.{Files, Paths}

import com.github.winmain.logserver.core.reader.{MemoryWiseLogWrapper, OldLogReader}
import com.github.winmain.logserver.core.storage.{AppendableBigStorage, RealDirectory}
import org.slf4j.Logger

import scala.collection.JavaConverters._

case class ConvertYearCommand() extends Command {
  /**
   * Сконвертировать год.
   * Например: dbDir = /mnt/test/logs/2015, yearDir = /home/myproject/log/sql/15
   * dbDir - Путь до базы данных логов
   * yearDir - Путь до исходных логов для указанного года
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length != 2) exitError("Usage: convert-year <db-dir> <old-month-dir>")

    val dbDir = Paths.get(params(0))
    val yearDir = Paths.get(params(1))

    val big: AppendableBigStorage = new AppendableBigStorage(new RealDirectory(dbDir))
    for (monthDir <- Files.newDirectoryStream(yearDir).asScala.filter(Files.isDirectory(_)).toVector.sortBy(_.getFileName.toString)) {
      log.info("Converting month: " + monthDir)
      new MemoryWiseLogWrapper(new OldLogReader(monthDir.toFile)).addRecords(big, log)
    }
    log.info("Closing BigStorage")
    big.close()

    log.info("Finished converting year")
  }
}
