package com.github.winmain.logserver.core
import com.github.winmain.logserver.core.storage.Storage
import com.github.winmain.logserver.core.storage.Storage.RecordId

trait SourceLogRecord {
  def tableName: String
  def id: RecordId
  def logBytesUTF8: Array[Byte]
  def timestamp: Long

  def occupiedMemory: Int = logBytesUTF8.length + 100

  def normalizedTableName: String = {
    val tn = tableName
    if (tn.startsWith("ros.")) tn.substring(4)
    else tn
  }

  def key: String = normalizedTableName + "/" + id
  def idKey: String = id.toString

  def toStorageRecord: Storage.Record =
    Storage.Record(timestamp, tableName, id, logBytesUTF8)

  def calcHash: Int = Storage.calcHash(logBytesUTF8, timestamp)
}
