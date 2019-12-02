package com.github.winmain.logserver.db

import java.nio.file.{Files, Path}

import com.github.winmain.logserver.core.{LogServer, RecordId}
import com.github.winmain.logserver.db.LogServerDb.{Info, JsRecord}
import com.github.winmain.logserver.db.reader.{MemoryWiseLogWrapper, NewLogReader}
import com.github.winmain.logserver.db.storage._
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

trait LogServerDb {
  def get(tableName: String, recordId: RecordId): Seq[JsRecord]

  def info(): Seq[Info]

  def archive(): Unit

  def update(paths: Seq[Path]): Unit
}

object LogServerDb {

  def create(dbDir: Path, log: Logger): LogServerDb = new Impl(dbDir, log)

  case class JsRecord(timestamp: Long, tableName: String, id: RecordId, data: String)

  case class Info(name: String,
                  readOnly: Boolean,
                  headVersion: Int,
                  records: Int,
                  headers: Int,
                  hashes: Int,
                  minTimestamp: Long,
                  maxTimestamp: Long)

  class LogServerError(msg: String) extends RuntimeException(msg)

  private class Impl(dbDir: Path, log: Logger) extends LogServerDb {

    def get(tableName: String, recordId: RecordId): Seq[JsRecord] = {
      val records = new ArrayBuffer[JsRecord]()

      if (!Files.isDirectory(dbDir)) throw new LogServerError("No database in dir " + dbDir.toAbsolutePath)
      val big = new ReadOnlyBigStorage(new RealDirectory(dbDir))
      if (big.storages.isEmpty) throw new LogServerError("No database in dir " + dbDir.toAbsolutePath)

      records ++= big.getRecords(tableName, recordId).map { r =>
        JsRecord(timestamp = r.timestamp, tableName = r.tableName, id = r.id, data = new Predef.String(r.data, LogServer.Charset))
      }
      big.close()

      records
    }

    def info(): Seq[Info] = {
      val big = new ReadOnlyBigStorage(new RealDirectory(dbDir))
      val infoList = big.storages.sortBy(_.info.name).map { storage =>
        val info: StorageInfo = storage.info
        val rrs: ReadOnceRecordStorage = new ReadOnceRecordStorage(storage.info.recordReadStream)
        val ehs: EssentialHeaderStorage = storage.ehs

        Info(
          info.name,
          ehs.isReadOnly,
          rrs.headVersion,
          rrs.headRecordNum,
          ehs.getCount,
          ehs.getHashCount,
          rrs.headMinTimestamp,
          rrs.headMaxTimestamp
        )
      }

      big.close()

      infoList
    }

    override def update(paths: Seq[Path]): Unit = {
      val big: AppendableBigStorage = new AppendableBigStorage(new RealDirectory(dbDir))
      val logReader: NewLogReader = new NewLogReader(paths, log)
      try {
        new MemoryWiseLogWrapper(logReader).addRecords(big, log)
      } finally {
        log.info("Closing BigStorage")
        big.close()

        log.info("Removing processed update files")
        logReader.processedSources.foreach(Files.delete)
      }
    }

    override def archive(): Unit = {
      val big: AppendableBigStorage = new AppendableBigStorage(new RealDirectory(dbDir))
      big.archive()
      big.close()
    }

  }

}
