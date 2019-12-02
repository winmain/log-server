package com.github.winmain.logserver.command

import java.nio.file.Paths

import com.fasterxml.jackson.databind.SerializationFeature
import com.github.winmain.logserver.core.RecordId
import com.github.winmain.logserver.db.LogServerDb
import com.github.winmain.logserver.db.utils.Js
import org.slf4j.Logger

case class GetCommand() extends Command {
  override def isVerbose: Boolean = false

  /**
   * Получить все записи в перечисленной базе данных
   * Например: dbDir = /mnt/test/logs/2015, tableName = user, recordId = 1
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length < 3) {
      exitError("Usage1: get <db-dir> <table-name> <record-id>\n" +
        "Usage2: get <base-db-dir> <table-name> <record-id> <year> [more-years...] ")
    }

    val baseDbDir = Paths.get(params(0))
    val tableName = params(1)
    val recordId = RecordId.parse(params(2))
    val years: Array[String] = params.drop(3)

    val records =
      if (years.nonEmpty)
        years
          .map(baseDbDir.resolve)
          .map(LogServerDb.create(_, log).get(tableName, recordId))
          .reduce(_ ++ _)
      else
        LogServerDb.create(baseDbDir, log).get(tableName, recordId)

    val mapper = Js.newMapper.configure(SerializationFeature.INDENT_OUTPUT, true)

    println(mapper.writeValueAsString(records.sortBy(_.timestamp)))
  }
}
