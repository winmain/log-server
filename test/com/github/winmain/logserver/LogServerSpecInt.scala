package com.github.winmain.logserver

import java.nio.file.{Files, Path}

import com.github.winmain.logserver.client.LogWriterClient
import com.github.winmain.logserver.core.RecordId
import com.github.winmain.logserver.db.LogServerDb
import com.github.winmain.logserver.db.LogServerDb.JsRecord
import com.google.common.jimfs.{Configuration, Jimfs}
import org.slf4j.Logger
import org.slf4j.impl.StaticLoggerBinder
import org.specs2.mutable.Specification

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

class LogServerSpecInt extends Specification {

  "Write & Update & Get & Archive" should {
    "Get written entries" in {
      val fs = Jimfs.newFileSystem(Configuration.unix())

      val updatesPath = fs.getPath("/updates")
      Files.createDirectory(updatesPath)

      val dbPath = fs.getPath("/db")
      Files.createDirectory(dbPath)

      val logger: Logger = StaticLoggerBinder.getSingleton.getLoggerFactory.getLogger("test")

      // WRITE

      val writer = new LogWriterClient(updatesPath, logger = logger)

      writer.append(userTableName, userId1, ts1, log1)
      writer.append(userTableName, userId1, ts2, log2)
      writer.append(userTableName, userId1, ts3, log3)
      writer.append(userTableName, userId2, ts4, log4)
      writer.append(userTableName, userId2, ts5, log5)
      writer.append(userTableName, userId2, ts5, log5)
      writer.append(userTableName, userId1, ts1, log1)
      writer.append(eventTableName, eventId1, ts6, log6)
      writer.append(eventTableName, eventId1, ts7, log7)

      fileNames(updatesPath) === List("current")

      writer.close()

      fileNames(updatesPath).length === 1
      fileNames(updatesPath).foreach(name => assert(name.endsWith(".saved")))

      // UPDATE

      val db = LogServerDb.create(dbPath, logger)
      db.update(Seq(updatesPath))

      fileNames(updatesPath) === List()
      fileNames(dbPath) === List("001.hash", "001.header", "001.record")

      // GET

      db.get(userTableName, RecordId(userId1)) === ArrayBuffer(
        JsRecord(ts1, userTableName, RecordId(userId1), log1),
        JsRecord(ts2, userTableName, RecordId(userId1), log2),
        JsRecord(ts3, userTableName, RecordId(userId1), log3)
      )

      db.get(userTableName, RecordId(userId2)) === ArrayBuffer(
        JsRecord(ts4, userTableName, RecordId(userId2), log4),
        JsRecord(ts5, userTableName, RecordId(userId2), log5)
      )

      db.get(eventTableName, RecordId(eventId1)) === ArrayBuffer(
        JsRecord(ts6, eventTableName, RecordId(eventId1), log6),
        JsRecord(ts7, eventTableName, RecordId(eventId1), log7)
      )

      // ARCHIVE

      db.archive()

      fileNames(dbPath) === List("001.hash", "001.header", "001.record.gz")

      // GET AGAIN FROM ARCHIVE

      db.get(userTableName, RecordId(userId1)) === ArrayBuffer(
        JsRecord(ts1, userTableName, RecordId(userId1), log1),
        JsRecord(ts2, userTableName, RecordId(userId1), log2),
        JsRecord(ts3, userTableName, RecordId(userId1), log3)
      )

      db.get(userTableName, RecordId(userId2)) === ArrayBuffer(
        JsRecord(ts4, userTableName, RecordId(userId2), log4),
        JsRecord(ts5, userTableName, RecordId(userId2), log5)
      )

      db.get(eventTableName, RecordId(eventId1)) === ArrayBuffer(
        JsRecord(ts6, eventTableName, RecordId(eventId1), log6),
        JsRecord(ts7, eventTableName, RecordId(eventId1), log7)
      )

      fs.close()

      success
    }
  }

  private def fileNames(path: Path): List[String] =
    Files
      .list(path)
      .map(_.getFileName)
      .iterator()
      .asScala
      .toList
      .map(_.toString)

  private val userTableName = "user"
  private val eventTableName = "event"

  private val userId1 = "user-1"
  private val userId2 = "user-2"

  private val eventId1 = 37377

  private val ts1 = 10L
  private val ts2 = 20L
  private val ts3 = 30L
  private val ts4 = 40L
  private val ts5 = 50L
  private val ts6 = 60L
  private val ts7 = 70L

  private val log1 = "Some user-related very important information 1"
  private val log2 = "Some user-related very important information 5678"
  private val log3 = "Some user-related very important information 73"
  private val log4 = "Some user-related very important information -----#"
  private val log5 = "Some user-related very important information 961"
  private val log6 = "EVENT!"
  private val log7 = "WOW! MORE EVENTS"

}
