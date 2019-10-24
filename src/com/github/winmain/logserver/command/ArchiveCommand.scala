package com.github.winmain.logserver.command
import java.nio.file.Paths

import com.github.winmain.logserver.core.storage.{RealDirectory, AppendableBigStorage}
import org.slf4j.Logger

case class ArchiveCommand() extends Command {
  /**
   * Заархивировать и перевести в read-only все хранилища записей.
   * Например: dbDir = /mnt/test/logs/2015
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length != 1) exitError("Usage: archive <db-dir>")

    val dbDir = Paths.get(params(0))

    val big: AppendableBigStorage = new AppendableBigStorage(new RealDirectory(dbDir))
    big.archive()
    big.close()

    log.info("Finished archiving")
  }
}
