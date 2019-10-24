package com.github.winmain.logserver.command
import java.nio.file.Paths

import com.github.winmain.logserver.db.LogServerDb
import com.github.winmain.logserver.db.storage.{AppendableBigStorage, RealDirectory}
import org.slf4j.Logger

case class ArchiveCommand() extends Command {
  /**
   * Заархивировать и перевести в read-only все хранилища записей.
   * Например: dbDir = /mnt/test/logs/2015
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length != 1) exitError("Usage: archive <db-dir>")

    val dbDir = Paths.get(params(0))

    LogServerDb.create(dbDir, log).archive()

    log.info("Finished archiving")
  }
}
