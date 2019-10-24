package command
import java.nio.file.{Files, Path, Paths}

import com.fasterxml.jackson.databind.SerializationFeature
import command.GetCommand.JsRecord
import core.storage.Storage.RecordId
import core.storage.{ReadOnlyBigStorage, RealDirectory, Storage}
import org.slf4j.Logger
import utils.Js

import scala.collection.mutable.ArrayBuffer

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

    val dbDirs: Seq[Path] =
      if (params.length > 3) {
        val years: Array[String] = params.drop(3)
        years.map(baseDbDir.resolve)
      } else Seq(baseDbDir)

    val mapper = Js.newMapper.configure(SerializationFeature.INDENT_OUTPUT, true)
    var records = new ArrayBuffer[JsRecord]()
    for (dbDir <- dbDirs) {
      if (!Files.isDirectory(dbDir)) exitError("No database in dir " + dbDir.toAbsolutePath)
      val big = new ReadOnlyBigStorage(new RealDirectory(dbDir))
      if (big.storages.isEmpty) exitError("No database in dir " + dbDir.toAbsolutePath)

      records ++= big.getRecords(tableName, recordId).map {r =>
        JsRecord(timestamp = r.timestamp, tableName = r.tableName, id = r.id, data = new Predef.String(r.data, Storage.Charset))
      }
      big.close()
    }
    println(mapper.writeValueAsString(records.sortBy(_.timestamp)))
  }
}


object GetCommand {
  case class JsRecord(timestamp: Long, tableName: String, id: RecordId, data: String)
}
