package com.github.winmain.logserver.core.storage
import java.util.concurrent.TimeUnit

import com.github.winmain.logserver.core.storage.Storage._
import org.slf4j.{Logger, LoggerFactory}

class StorageOpts {
  /**
   * Максимальное, значение которое можно записать 32 битами,
   */
  var maxOffset: Long = Int.MaxValue

  /**
   * Выбираем такое значение, чтобы нам хватало памяти хранить все хедеры и хеши в памяти
   * хотябы для двух полных хранилищ, чтобы добавление новых записей работало бесперебойно.
   * В первое хранилище мы активно пишем новые записи, а второе открыто в readOnly, и используется
   * для поиска совпадений по хешу.
   * Каждый хедер занимает 8 байт, каждый хеш 8 байт. Суммарно получаем 16 байт, необходимых
   * для хранения хедера с хешем для одной записи.
   * Для выбранного значения 10М получаем 10М * (8+8) * 2 = 320МБ - требуемый объём памяти для двух
   * полных хранилищ.
   * Этот расчёт был для уже упакованного хранилища. Если же мы в процессе заполнения хранилища,
   * то эмпирическим путём выяснено, что сервер может восстановить примерно 4155000 записей при
   * -Xmx=1G, и после этого получить OutOfMemoryError. Поэтому, потолок здесь снижен до 2М записей.
   */
  var maxRecordNum: Int = 2000000
  var recordStorageVersion: Int = 2
  var recordStorageHeaderSize: Int = 64

  /**
   * Добавить хук, который разблокирует [[BigStorage]] при остановке JVM.
   */
  var addBigStorageUnlockHook: Boolean = true

  /**
   * Время ожидания разблокировки [[BigStorage]], если он заблокирован, прежде чем бросить ошибку.
   */
  var openLockWaitTimeout: (Int, TimeUnit) = 60 -> TimeUnit.SECONDS
}

object DefaultStorageOpts {
  def apply(isDev: Boolean): StorageOpts = {
    val opts = new StorageOpts
    if (isDev) opts.openLockWaitTimeout = 0 -> TimeUnit.SECONDS
    opts
  }
}


abstract class RecordStorage(opts: StorageOpts) {
  /**
   * Заголовок, описывающий файл хранилища.
   * Находится в начале файла, и всегда занимает [[opts.recordStorageHeaderSize]] байт.
   */
  class RSHead {
    var version: Int = opts.recordStorageVersion
    var totalBytes: Long = opts.recordStorageHeaderSize
    var minTimestamp: Long = Long.MaxValue
    var maxTimestamp: Long = 0L
    var recordNum: Int = 0

    var needSave: Boolean = false

    def initFrom(read: ReadStream): Unit = {
      version = read.getInt
      require(version == opts.recordStorageVersion, "Unknown version: " + version)
      totalBytes = read.getLong
      minTimestamp = read.getLong
      maxTimestamp = read.getLong
      recordNum = read.getInt
      needSave = false
      read.skip(opts.recordStorageHeaderSize - read.pos)
    }
    def write(rw: ReadWrite, writeEndingZeroes: Boolean = false): Unit = {
      val pos0: Long = rw.pos
      rw.putInt(version)
      rw.putLong(totalBytes)
      rw.putLong(minTimestamp)
      rw.putLong(maxTimestamp)
      rw.putInt(recordNum)
      val size: Long = rw.pos - pos0
      require(size <= opts.recordStorageHeaderSize, "RSHead too big: " + size + " > " + opts.recordStorageHeaderSize)
      if (writeEndingZeroes) {
        var i = opts.recordStorageHeaderSize - size
        while (i > 7) {rw.putLong(0L); i -= 8}
        while (i > 0) {rw.putByte(0); i -= 1}
      }
      needSave = false
    }

    def onRecordAdded(record: Record, newTotalBytes: Long): Unit = {
      require(newTotalBytes > totalBytes, "newTotalBytes must be greater than totalBytes: " + newTotalBytes + ", " + totalBytes)
      totalBytes = newTotalBytes
      minTimestamp = math.min(minTimestamp, record.timestamp)
      maxTimestamp = math.max(maxTimestamp, record.timestamp)
      recordNum += 1
      needSave = true
    }
  }

  protected var head = new RSHead
  def headVersion: Int = head.version
  def headTotalBytes: Long = head.totalBytes
  def headMinTimestamp: Long = head.minTimestamp
  def headMaxTimestamp: Long = head.maxTimestamp
  def headRecordNum: Int = head.recordNum
  def headNeedSave: Boolean = head.needSave

  protected var endFileOffset: Long = opts.recordStorageHeaderSize

  /**
   * Записать запись в хранилище, и увеличить offset на величину записанных байт.
   * Также, проверяет, не будет ли превышения размера хранилища.
   *
   * @param record Запись
   * @return Если запись прошла успешно, и в хранилище ещё есть место, то возвращаем Some(buf) -
   *         буфер, в который прошла эта запись. Сам буфер возвращается в состоянии записи с позицией
   *         в конце данных (не флипованный).
   *         Если же свободного места нет, и запись не прошла, то возвращаем None. В таком случае,
   *         нужно создавать следующее хранилище.
   */
  protected def writeRecord(record: Record, rw: ReadWrite): Boolean = {
    val tableNameBytes: Array[Byte] = record.tableName.getBytes(Charset)
    val size: Long = 8L + 4 + tableNameBytes.length + record.id.length + 4 + record.data.length
    if (endFileOffset + size > opts.maxOffset) false
    else {
      val pos0 = rw.pos
      rw.putLong(record.timestamp)
      writeBytes(rw, tableNameBytes)
      rw.putRecordId(record.id)
      writeBytes(rw, record.data)
      val pos = rw.pos
      rw.truncate(pos)
      val writtenSize = pos - pos0
      require(size == writtenSize, "Calculated size not equals to written size: " + size + " != " + writtenSize)
      endFileOffset += size
      head.onRecordAdded(record, endFileOffset)
      true
    }
  }

  /**
   * Прочитать запись из хранилища, пропустив чтение размера
   * @param read Буфер, откуда идёт чтение записи
   * @return Прочитанная запись
   */
  protected def readRecord(read: ReadStream): Record = {
    val timestamp: Long = read.getLong
    val tableNameBytes = readBytes(read)
    val id = read.getRecordId
    val dataBytes = readBytes(read)

    Record(timestamp, new String(tableNameBytes, Charset), id, dataBytes)
  }

  /**
   * Восстановить заголовки из хранилища и сам [[RSHead]].
   */
  protected def restore(read: ReadStream, log: Logger)(receiver: Header => Any): RSHead = {
    val newHead = new RSHead
    endFileOffset = opts.recordStorageHeaderSize
    while (read.available) {
      val pos0 = read.pos
      try {
        val record: Record = readRecord(read)
        receiver(Header(record.timestamp, endFileOffset.toInt, record.calcHash, record.tableName, record.id))
        endFileOffset += read.pos - pos0
        newHead.onRecordAdded(record, endFileOffset)
      } catch {
        case IoDataStreamException(e) =>
          log.error("Cannot read crashed record at end of file " + read + " starting at pos:" + pos0 + ". Deleting it & truncating file.")
      }
    }
    newHead
  }
}


/**
 * Реализация хранилища, поддерживающая только чтение записей из одного потока.
 * Эту реализацию можно использовать для чтения хранилища из GZIPInputStream.
 */
class ReadOnceRecordStorage(read: ReadStream, opts: StorageOpts = new StorageOpts) extends RecordStorage(opts) {
  head.initFrom(read)

  def readRecord(offset: Int): Record = {
    require(read.pos <= offset, "Cannot read before ReadStream position")
    read.skip(offset - read.pos)
    readRecord(read)
  }

  def readRecords(offsets: Seq[Int]): Seq[Record] = offsets.sorted.map(readRecord)

  def close(): Unit = read.close()
}

/**
 * Специальная версия хранилища, которая служит только для того чтобы получить все хедеры для восстановления.
 *
 * @param read Источник хранилища, его мы читаем целиком, чтобы восстановить данные.
 * @param rwFn Функция, возвращающая [[ReadWrite]] для записи самого хранилища - на случай
 *             если нужно обновить хедеры этого хранилища.
 */
class RecoveryRecordStorage(read: ReadStream,
                            rwFn: () => ReadWrite,
                            opts: StorageOpts = new StorageOpts,
                            log: Logger = LoggerFactory.getLogger(getClass)) extends RecordStorage(opts) {
  head.initFrom(read)

  def usingReceiver(receiver: Header => Any): Unit = {
    checkAndSaveHead(restore(read, log)(receiver))
    read.close()
  }

  def toHeaderStorage: NewHeaderStorage = {
    val hs: NewHeaderStorage = new NewHeaderStorage()
    checkAndSaveHead(restore(read, log) {header =>
      hs.add(header, allowDuplicates = true)
    })
    read.close()
    hs
  }

  private def checkAndSaveHead(newHead: RSHead): Unit = {
    if (newHead.totalBytes != head.totalBytes || newHead.recordNum != head.recordNum) {
      val rw: ReadWrite = rwFn()
      rw.seek(0)
      newHead.write(rw, writeEndingZeroes = true)
      rw.close()
    }
  }
}


/**
 * Реализация хранилища, которая поддерживает как чтение, так и добавление новых записей.
 * При старте проверяет, не устарел ли [[RecordStorage.RSHead]] - тот случай, когда новые записи
 * добавились, а head не обновился из-за некорректного закрытия хранилища. В таком случае,
 * хранилище генерирует warning и восстанавливает хедеры через [[onRecoverReceiver]].
 */
class AppendableRecordStorage(rw: ReadWrite,
                              opts: StorageOpts = new StorageOpts,
                              log: Logger = LoggerFactory.getLogger(getClass),
                              onRecoverReceiver: => Header => Any = h => h) extends RecordStorage(opts) {
  if (rw.available) {
    head.initFrom(rw)
    if (head.totalBytes != rw.length) {
      // Простейшая проверка на устаревание head - если длина файла не совпадает, значит head
      // можно выбросить, и начать восстановление всех хедеров записей.
      log.warn("Storage " + rw.filePath + " has invalid head.totalBytes: " + head.totalBytes +
        ", but storageFile.length: " + rw.length + ". Restoring storage & headers.")
      val receiver: (Header) => Any = onRecoverReceiver
      head = restore(rw, log)(receiver)
      head.needSave = true
    } else {
      endFileOffset = head.totalBytes
    }
  } else {
    rw.seek(0)
    head.write(rw, writeEndingZeroes = true)
    endFileOffset = head.totalBytes
  }

  def readRecord(offset: Int): Record = {
    rw.seek(offset)
    readRecord(rw)
  }

  /**
   * Добавить запись в хранилище.
   * Возвращает Some(offset), если запись успешно добавлена,
   * либо None, если место в хранилище закончилось.
   */
  def addRecord(record: Record): Option[Int] = {
    if (head.recordNum >= opts.maxRecordNum) None
    else {
      val offset: Long = endFileOffset
      rw.seek(offset)
      if (writeRecord(record, rw)) Some(offset.toInt)
      else None
    }
  }

  def writeHead(): Unit = {
    if (head.needSave) {
      rw.seek(0)
      head.write(rw)
    }
  }

  def close(): Unit = {
    if (head.needSave) writeHead()
    rw.close()
  }
}
