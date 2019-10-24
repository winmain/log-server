package com.github.winmain.logserver.command
import java.nio.file.Paths
import java.time.format.DateTimeFormatter

import com.github.winmain.logserver.db.LogServerDb
import com.github.winmain.logserver.db.utils.Dates
import org.slf4j.Logger

case class InfoCommand() extends Command {
  private val DateFormat = DateTimeFormatter.ofPattern("yy-MM-dd_HH:mm:ss")

  /**
   * Получить информацию по базе данных, которая находится в заданном каталоге dbDir.
   * Например: dbDir = /mnt/test/logs
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length < 1) exitError("Usage: info <db-dir>")

    val dbDir = Paths.get(params(0))
    val infos = LogServerDb.create(dbDir, log).info()

    infos.foreach {info =>
      println(info.name + ": r/o:" + (if (info.readOnly) 1 else 0) +
        " v:" + info.headVersion +
        " records:" + info.records +
        " headers:" + info.headers +
        " hashes:" + info.hashes +
        " minTS:" + DateFormat.format(Dates.toLocalDateTime(info.minTimestamp)) +
        " maxTS:" + DateFormat.format(Dates.toLocalDateTime(info.maxTimestamp))
      )
    }

    println("Total storages: " + infos.size)
  }
}
