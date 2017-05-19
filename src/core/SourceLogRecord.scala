package core
import core.storage.Storage

trait SourceLogRecord {
  def tableName: String
  def id: Option[Int]
  def logBytesUTF8: Array[Byte]
  def timestamp: Long

  def occupiedMemory: Int = logBytesUTF8.length + 100

  def normalizedTableName: String = {
    val tn = tableName
    if (tn.startsWith("ros.")) tn.substring(4)
    else tn
  }

  def key: String = id.fold(normalizedTableName)(normalizedTableName + "/" + _)
  def idKey: String = id.fold("-")(_.toString)

  def toStorageRecord: Storage.Record =
    Storage.Record(timestamp, tableName, id.getOrElse(0), logBytesUTF8)

  def calcHash: Int = Storage.calcHash(logBytesUTF8, timestamp)
}
