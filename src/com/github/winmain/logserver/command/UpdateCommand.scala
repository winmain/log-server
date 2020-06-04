package com.github.winmain.logserver.command
import java.nio.file.{Path, Paths}

import com.github.winmain.logserver.db.LogServerDb
import org.slf4j.Logger

case class UpdateCommand() extends Command {
  /**
   * Добавить новые данные в базу и удалить успешно прочитанные данные логов.
   * Например: dbDir = /mnt/test/logs/2015, updates = /home/myproject/log/sql-new
   * dbDir - Путь до базы данных логов с годом вконце
   * updates - Обновления - каталог с обновлениями, либо сами файлы обновлений
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length < 2) exitError("Usage: update <db-dir> <updates>")

    val dbDir = Paths.get(params(0))
    val updatePaths: Vector[Path] = params.drop(1).map(Paths.get(_)).toVector

    LogServerDb.create(dbDir, log).update(updatePaths)

    log.info("Finished update")
  }
}
