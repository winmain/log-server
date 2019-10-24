package com.github.winmain.logserver.core.storage
import com.github.winmain.logserver.core.RecordId
import com.github.winmain.logserver.core.storage.Storage._
import javax.annotation.concurrent.NotThreadSafe
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.ref.SoftReference

/**
 * Большое хранилище. Объединяет доступ к нескольким хранилищам (записи + хедеры), находящимся
 * в одном каталоге.
 *
 * @param dir Каталог с данными хранилища
 * @param opts Опции хранилищ записей
 * @param log Логгер
 */
@NotThreadSafe
abstract class BigStorage(dir: Directory,
                          opts: StorageOpts,
                          log: Logger) {

  abstract class Storage(val info: StorageInfo) {
    def ehs: EssentialHeaderStorage
    def hs: HeaderStorage
    def canBeAppendable: Boolean = !info.gzipped && !ehs.isReadOnly
    def includeTimestamp(ts: Long): Boolean = ehs.getMinTimestamp <= ts && ts <= ehs.getMaxTimestamp
    def containsRecord(record: Record, hash: Int): Boolean = {
      if (includeTimestamp(record.timestamp)) {
        var hs = this.hs
        if (!hs.hasValidHashes) {
          hs = recoverAndSaveHeaderStorage("Storage.containsRecord() no valid hashes")
          updateHs(hs)
        }
        hs.contains(record.tableName, record.id, hash)
      } else false
    }
    def close(): StorageInfo

    def recoverAndSaveHeaderStorage(reason: String): NewHeaderStorage = {
      val newHs: NewHeaderStorage = new RecoveryRecordStorage(info.recordReadStream, () => info.recordReadWrite, opts).toHeaderStorage
      newHs.save(info.headerReadWrite, Some(info.hashReadWrite))
      if (newHs.getCount > 0) log.warn("Recovered headers for " + info + ", reason:" + reason)
      newHs
    }

    protected def updateHs(newHs: HeaderStorage)
  }


  class ReadOnlyStorage(info: StorageInfo) extends Storage(info) {
    private var maybeEhs: Option[EssentialHeaderStorage] = None
    private var hsRef: SoftReference[HeaderStorage] = new SoftReference(null: HeaderStorage)
    private var hsRefWasSet: Boolean = false

    private def setEhs(): EssentialHeaderStorage = {
      maybeEhs = Some {
        val headerRS: ReadStream = info.headerReadStream
        if (headerRS.available)
          checkAndRecoverHeaders(new EssentialHeaderStorageImpl(headerRS), "ReadOnlyStorage non-updated EssentialHeaderStorage")
        else recoverAndSaveHeaderStorage("ReadOnlyStorage read EssentialHeaderStorage")
      }
      maybeEhs.get
    }

    private def updateHsRef(): HeaderStorage = {
      val headerRS: ReadStream = info.headerReadStream
      val hs: HeaderStorage =
        if (headerRS.available)
          checkAndRecoverHeaders(new ExistedHeaderStorage(headerRS, info.hashReadStream), "ReadOnlyStorage non-updated HeaderStorage").asInstanceOf[HeaderStorage]
        else recoverAndSaveHeaderStorage("ReadOnlyStorage updateHsRef")
      updateHs(hs)
      hs
    }

    override def ehs: EssentialHeaderStorage = hsRef.get.getOrElse(maybeEhs.getOrElse(setEhs()))
    override def hs: HeaderStorage = hsRef.get.getOrElse(updateHsRef())

    private def checkAndRecoverHeaders(newEhsFn: => EssentialHeaderStorage, recoverReason: String): EssentialHeaderStorage = {
      val newEhs: EssentialHeaderStorage =
        try newEhsFn
        catch {case IoDataStreamException(e) => return recoverAndSaveHeaderStorage(recoverReason)}
      val rrs: ReadOnceRecordStorage = new ReadOnceRecordStorage(info.recordReadStream, opts)
      val recordNum: Int = rrs.headRecordNum
      rrs.close()
      if (recordNum != newEhs.getCount)
        recoverAndSaveHeaderStorage(recoverReason + ", " + recordNum + " != " + newEhs.getCount)
      else newEhs
    }

    override def close(): StorageInfo = {
      maybeEhs = None
      hsRef.clear()
      hsRefWasSet = false
      info
    }

    override protected def updateHs(newHs: HeaderStorage): Unit = {
      if (hsRefWasSet && hsRef.get.isEmpty) {
        log.info("Restoring evicted storage " + info)
      }
      hsRefWasSet = true
      hsRef = new SoftReference(newHs)
    }
  }

  /**
   * Проверить и восстановить все стораджи.
   * Это обязательное действие при открытии стораджей, чтобы не возникало ошибок потом.
   */
  protected def validateAndRecoverStorages(): Unit = {
    for (info <- dir.infos) {
      val recordRead: ReadStream = info.recordReadStream
      val recordMaybeLength: Option[Long] = recordRead.maybeLength
      val rrs: RecoveryRecordStorage = new RecoveryRecordStorage(recordRead, () => info.recordReadWrite, opts, log)

      def doRecover(reason:String): Unit = {
        log.warn("Recovering headers for " + info + ", reason:" + reason)
        val newHs: NewHeaderStorage = rrs.toHeaderStorage
        newHs.save(info.headerReadWrite, Some(info.hashReadWrite))
      }

      if (recordMaybeLength.exists(_ != rrs.headTotalBytes))
        doRecover("rrs.headTotalBytes:" + rrs.headTotalBytes + " != filesize:" + recordMaybeLength.get)
      else {
        val headerRS: ReadStream = info.headerReadStream
        if (headerRS.available) {
          val ehs: EssentialHeaderStorageImpl = new EssentialHeaderStorageImpl(headerRS)
          if (ehs.getCount != rrs.headRecordNum) doRecover("ehs.getCount:" + ehs.getCount + " != rrs.headRecordNum:" + rrs.headRecordNum)
          else if (ehs.getMaxTimestamp != rrs.headMaxTimestamp || ehs.getMinTimestamp != rrs.headMinTimestamp) doRecover("Timestamps not synced")
        } else {
          doRecover("No headers")
        }
      }
    }
  }

  def locked: Boolean = dir.locked
  protected def requireLocked(): Unit = require(locked, "Cannot lock BigStorage")
  protected[storage] def unlock() = dir.unlock()
  def close(): Unit = unlock()

  dir.lock(log, opts.openLockWaitTimeout)
  if (opts.addBigStorageUnlockHook) BigStorageLocks.add(dir)

  validateAndRecoverStorages()
}


/**
 * Большое хранилище, которое умеет только получать записи.
 * Если хедеры или хешы отсутствуют или повреждены, то они будут восстановлены в процессе поиска.
 *
 * @param dir Каталог с данными хранилища
 * @param opts Опции хранилищ записей
 * @param log Логгер
 */
@NotThreadSafe
class ReadOnlyBigStorage(dir: Directory,
                         opts: StorageOpts = new StorageOpts,
                         log: Logger = LoggerFactory.getLogger(classOf[BigStorage])) extends BigStorage(dir, opts, log) {
  val storages: Vector[Storage] = dir.infos.map(new ReadOnlyStorage(_))(collection.breakOut)

  def getRecords(tableName: String, id: RecordId): Vector[Record] = {
    requireLocked()
    val result = Vector.newBuilder[Record]
    for (storage <- storages) {
      val hs: HeaderStorage = {
        storage.hs match {
          case s if s.getCount > 0 => s
          case _ =>
            // Хедеры отсутствуют. Попробовать восстановить их.
            storage.recoverAndSaveHeaderStorage("getRecords " + tableName + ":" + id)
        }
      }
      val offsets: Vector[Int] = hs.getOffsets(tableName, id)
      if (offsets.nonEmpty) {
        val rs: ReadOnceRecordStorage = new ReadOnceRecordStorage(storage.info.recordReadStream, opts)
        result ++= rs.readRecords(offsets)
        rs.close()
      }
    }
    result.result()
  }

  override def close(): Unit = {
    storages.foreach(_.close())
    super.close()
  }
}


/**
 * Большое хранилище, открытое в режиме добавления новых записей.
 * Если хедеры или хешы отсутствуют или повреждены, то они будут восстановлены.
 *
 * @param dir Каталог с данными хранилища
 * @param opts Опции хранилищ записей
 * @param log Логгер
 */
@NotThreadSafe
class AppendableBigStorage(dir: Directory,
                           opts: StorageOpts = new StorageOpts,
                           log: Logger = LoggerFactory.getLogger(classOf[BigStorage])) extends BigStorage(dir, opts, log) {
  class AppendableStorage(info: StorageInfo) extends Storage(info) {
    require(!info.gzipped, "Cannot make AppendableStorage over gzipped StorageInfo: " + info)

    private var _hs: HeaderStorage = null

    private[storage] val ars: AppendableRecordStorage =
      new AppendableRecordStorage(info.recordReadWrite, opts, log, onRecoverReceiver = {
        // Хедеры здесь восстанавливаются, если заголовок RecordStorage не соотносится с его содержимым.
        // В таком случае, мы полагаем, что RecordStorage был некорректно закрыт, и хедеры скорей всего
        // некорректны. Поэтому, мы их явно восстанавливаем.
        _hs = new NewHeaderStorage()
        _hs.add(_, allowDuplicates = true)
      })

    if (_hs == null) {
      val hasRecords: Boolean = info.recordReadStream.available
      if (hasRecords) {
        val headerRS: ReadStream = info.headerReadStream
        val rrs: ReadOnceRecordStorage = new ReadOnceRecordStorage(info.recordReadStream, opts)
        if (headerRS.available) {
          val existedHS: ExistedHeaderStorage = new ExistedHeaderStorage(headerRS, info.hashReadStream)
          if (existedHS.getCount != rrs.headRecordNum) {
            _hs = recoverAndSaveHeaderStorage("Invalid count in AppendableStorage, got " + existedHS.getCount + ", must be " + rrs.headRecordNum) // Восстановить устаревшие хедеры
          } else _hs = existedHS // Актуальные хедеры
        } else _hs = recoverAndSaveHeaderStorage("No headers in AppendableStorage") // Восстановить отсутствующие хедеры
      }
      else _hs = new NewHeaderStorage
    }

    if (_hs.needSave) _hs.save(info.headerReadWrite, Some(info.hashReadWrite))
    if (ars.headNeedSave) ars.writeHead()

    require(!_hs.isReadOnly, "Cannot make AppendableStorage with readOnly HeaderStorage: " + info)

    override def ehs: EssentialHeaderStorage = _hs
    override def hs: HeaderStorage = _hs

    /**
     * Добавить запись в хранилище записей и хедеров.
     * Возвращает true, если запись успешно добавлена, и false, если хранилище заполнено, и запись
     * добавить уже нельзя.
     */
    def addRecord(record: Record): Boolean = {
      ars.addRecord(record) match {
        case Some(offset) =>
          val header: Header = record.makeHeader(offset)
          require(hs.add(header, allowDuplicates = true), "Cannot add header, duplicate found")
          true
        case None =>
          hs.setReadOnly(true)
          false
      }
    }

    /**
     * Сжать файл хранилища записей
     */
    def archive(): StorageInfo = {
      if (!info.gzipped) {
        log.info("Gzipping " + info)
        dir.gzipInfo(info)
      } else
        info
    }

    override def close(): StorageInfo = {
      ars.close()
      if (hs.needSave) {
        hs.save(info.headerReadWrite, Some(info.hashReadWrite))
      }
      // Сжать файл хранилища записей, если в него уже ничего нельзя записать
      if (hs.isReadOnly && !info.gzipped) archive()
      else info
    }

    override protected def updateHs(newHs: HeaderStorage): Unit = _hs = newHs
  }

  private[storage] val storages: mutable.ArrayBuffer[Storage] = dir.infos.map(new ReadOnlyStorage(_))(collection.breakOut)
  private var appendStorageIdx = 0

  private def containsRecord(record: Record, hash: Int): Boolean = storages.exists(_.containsRecord(record, hash))

  private def findOrMakeAppendableStorage: AppendableStorage = {
    while (appendStorageIdx < storages.size) {
      storages(appendStorageIdx) match {
        case st: AppendableStorage =>
          if (!st.hs.isReadOnly) return st
        case st: ReadOnlyStorage =>
          if (st.canBeAppendable) {
            val storage = new AppendableStorage(st.info)
            storages(appendStorageIdx) = storage
            return storage
          }
      }
      appendStorageIdx += 1
    }
    // Если не найден ни один подходящий storage, создать новый
    val newInfo: StorageInfo = dir.addNewInfo()
    log.info("Create new storage: " + newInfo)
    val storage: AppendableStorage = new AppendableStorage(newInfo)
    storages += storage
    storage
  }

  def addRecord(record: Record): Boolean = {
    requireLocked()
    val hash: Int = record.calcHash
    if (containsRecord(record, hash)) false
    else {
      def tryAdd(): Unit = {
        val storage: AppendableStorage = findOrMakeAppendableStorage
        if (!storage.addRecord(record)) {
          // Закрыть и сконвертировать AppendableStorage обратно в ReadOnlyStorage для экономии памяти
          val info: StorageInfo = storage.close()
          storages(appendStorageIdx) = new ReadOnlyStorage(info)
          tryAdd()
        }
      }
      tryAdd()
      true
    }
  }

  /**
   * Заархивировать в gzip все несжатые хранилища.
   */
  def archive(): Unit = {
    for {i <- storages.indices
         storage = storages(i) if !storage.info.gzipped} {
      storage match {
        case st: AppendableStorage =>
          storages(i) = new ReadOnlyStorage(st.archive())
        case st: ReadOnlyStorage =>
          if (st.canBeAppendable) {
            storages(i) = new ReadOnlyStorage(new AppendableStorage(st.info).archive())
          }
      }
    }
  }

  override def close() = {
    storages.foreach(_.close())
    super.close()
  }
}



object BigStorageLocks {
  private var lockedDirs = new ArrayBuffer[Directory]()

  def add(dir: Directory): Unit = {
    if (!lockedDirs.contains(dir)) lockedDirs += dir
  }

  def unlockAll(): Unit = lockedDirs.foreach(_.unlock())

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = unlockAll()
  })
}
