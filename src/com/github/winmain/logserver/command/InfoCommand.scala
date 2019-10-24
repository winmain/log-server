package com.github.winmain.logserver.command
import java.nio.file.Paths
import java.time.format.DateTimeFormatter

import com.github.winmain.logserver.core.storage._
import org.slf4j.Logger
import com.github.winmain.logserver.utils.Dates

case class InfoCommand() extends Command {
  private val DateFormat = DateTimeFormatter.ofPattern("yy-MM-dd_HH:mm:ss")

  /**
   * Получить информацию по базе данных, которая находится в заданном каталоге dbDir.
   * Например: dbDir = /mnt/test/logs
   */
  override def run(log: Logger, params: Array[String]): Unit = {
    if (params.length < 1) exitError("Usage: info <db-dir>")

    val dbDir = Paths.get(params(0))
    val big = new ReadOnlyBigStorage(new RealDirectory(dbDir))
    big.storages.sortBy(_.info.name).foreach {storage =>
      val info: StorageInfo = storage.info
      val rrs: ReadOnceRecordStorage = new ReadOnceRecordStorage(storage.info.recordReadStream)
      val ehs: EssentialHeaderStorage = storage.ehs
      println(info.name + ": r/o:" + (if (ehs.isReadOnly) 1 else 0) +
        " v:" + rrs.headVersion +
        " records:" + rrs.headRecordNum +
        " headers:" + ehs.getCount +
        " hashes:" + ehs.getHashCount +
        " minTS:" + DateFormat.format(Dates.toLocalDateTime(rrs.headMinTimestamp)) +
        " maxTS:" + DateFormat.format(Dates.toLocalDateTime(rrs.headMaxTimestamp))
      )
    }
    println("Total storages: " + big.storages.size)
    big.close()
  }
}
