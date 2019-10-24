package com.github.winmain.logserver.core.storage
import java.util.function.LongConsumer

import com.github.winmain.logserver.core.RecordId
import com.github.winmain.logserver.core.storage.Storage._
import com.koloboke.collect.map.hash.{HashIntObjMap, HashIntObjMaps, HashObjIntMap, HashObjIntMaps}
import com.koloboke.collect.set.hash.{HashLongSet, HashLongSets}
import com.koloboke.function.IntObjConsumer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait EssentialHeaderStorage {
  def isReadOnly: Boolean
  def getMinTimestamp: Long
  def getMaxTimestamp: Long
  def getCount: Int
  def getHashCount: Int
}

/**
 * Базовый класс хранилища хедеров, которые состоят из оффсетов (позиция в файле записей)
 * и хешей (служат для быстрого поиска дубликатов).
 */
abstract class HeaderStorage extends EssentialHeaderStorage {
  /** Имена таблиц, для получения id по имени таблицы */
  val tableNames: TableNames

  protected var readOnly: Boolean = false
  protected var count: Int = 0
  protected var savedCount: Int = 0
  protected var hashCount: Int = 0
  protected var minTimestamp: Long = Long.MaxValue
  protected var maxTimestamp: Long = 0L

  protected var _needSave: Boolean = false

  /**
   * Хранилище только для чтения? Флаг действует как на хранилище заголовков, так и на хранилище записей.
   * Он устанавливается, когда хранилище записей заархивировано, либо имеет слишком большой размер,
   * что дальнейшая запись в него невозможна.
   */
  override def isReadOnly: Boolean = readOnly
  def setReadOnly(v: Boolean): Unit = {
    if (readOnly != v) _needSave = true
    readOnly = v
  }

  /** Количество хедеров */
  override def getCount: Int = count
  /** Количество хешей */
  override def getHashCount: Int = hashCount
  override def getMinTimestamp: Long = minTimestamp
  override def getMaxTimestamp: Long = maxTimestamp

  /**
   * Это хранилище может использовать хеши, которые были корректно прочитаны при инициализации?
   * Использование хешей даёт возможность поиска записей в методе [[contains()]]
   */
  def hasValidHashes: Boolean

  /**
   * Сортированный массив для сжатого хранения table, id, offset.
   * Каждое значение состоит из верхних 4 байт: table+id, и нижних 4 байт: offset.
   * Служит для быстрого поиска offset по table+id используя [[java.util.Arrays.binarySearch()]].
   */
  protected var tableIdOffsets: LongByteBuffer = EmptyLongByteBuffer

  def calcTableIdOffset(tablePlusId: Int, offset: Int): Long = (tablePlusId.toLong << 32) | offset

  /**
   * Сортированный массив для сжатого хранения table, id, hash.
   * Каждое значение состоит из верхних 4 байт: table+id, и нижних 4 байт: hash.
   * Служит для быстрого определения, существует ли запись с таким хешем используя [[java.util.Arrays.binarySearch()]].
   * None означает, что хеши недоступны, т.е. файл с хешами не существует, либо не прочитан.
   */
  protected var tableIdHashes: LongByteBuffer = EmptyLongByteBuffer

  def calcTableIdHash(tablePlusId: Int, hash: Int): Long = (tablePlusId.toLong << 32) | hash

  /** Новые добавленные значения table+id, offset. */
  protected val newTableIdOffsets: HashIntObjMap[List[Int]] = HashIntObjMaps.newUpdatableMap()
  /** Новые добавленные значения table+id, hash. */
  protected val newTableIdHashes: HashLongSet = HashLongSets.newUpdatableSet()

  /**
   * Найти значение table+id, по сути это простейшая упаковка tableId и id в одно Int-значение.
   */
  def calcTablePlusId(tableId: Int, id: RecordId): Int = Integer.reverse(tableId) ^ id.hash

  /**
   * Есть ли запись с таким хешем?
   * Этот метод работает только если хеши были загружены. Иначе, он бросает exception.
   *
   * @param tablePlusId Сжатое значение table+id
   * @param hash Хеш записи для поиска
   */
  def contains(tablePlusId: Int, hash: Int): Boolean = {
    if (!hasValidHashes) sys.error("No hashes loaded, cannot check header existence")
    val tableIdHash: Long = calcTableIdHash(tablePlusId, hash)
    tableIdHashes.binarySearch(tableIdHash) >= 0 || newTableIdHashes.contains(tableIdHash)
  }

  /**
   * Есть ли запись с таким хешем?
   * Этот метод работает только если хеши были загружены. Иначе, он бросает exception.
   */
  def contains(tableName: String, id: RecordId, hash: Int): Boolean = {
    tableNames.get(tableName) match {
      case -1 => false
      case tableId => contains(calcTablePlusId(tableId, id), hash)
    }
  }

  /**
   * Найти все оффсеты по table+id.
   */
  def getOffsets(tablePlusId: Int): Vector[Int] = {
    val array: LongByteBuffer = tableIdOffsets
    var idx: Int =
      array.binarySearch(calcTableIdOffset(tablePlusId, 0)) match {
        case i if i < 0 => -i - 1
        case i => i
      }
    @inline def getTablePlusId(packedLong: Long): Int = (packedLong >> 32).toInt
    @inline def getOffset(packedLong: Long): Int = (packedLong & 0xffffffff).toInt
    // Находим начальный и конечный индекс диапазона оффсетов
    idx = math.max(0, idx - 1)
    while (idx >= 0 && idx < array.length && getTablePlusId(array(idx)) == tablePlusId) idx -= 1
    var end = idx + 1
    while (end < array.length && getTablePlusId(array(end)) == tablePlusId) end += 1

    val rb = Vector.newBuilder[Int]
    idx += 1
    while (idx < end) {
      rb += getOffset(array(idx))
      idx += 1
    }

    // Добавим новые оффсеты, если такие есть
    newTableIdOffsets.get(tablePlusId) match {
      case null =>
      case toAdd => rb ++= toAdd
    }
    rb.result()
  }

  /**
   * Найти все оффсеты для заданной записи.
   */
  def getOffsets(tableName: String, id: RecordId): Vector[Int] = {
    tableNames.get(tableName) match {
      case -1 => Vector.empty
      case tableId =>
        getOffsets(calcTablePlusId(tableId, id))
    }
  }


  /**
   * Добавить новый хедер. Если хедер с таким table+id+hash уже есть, то новый не будет добавлен,
   * и метод вернёт false. В случае успешного добавления, метод вернёт true.
   *
   * @param header Добавляемый хедер
   * @param allowDuplicates Если указан, то дубликаты также будут добавлены
   */
  def add(header: Header, allowDuplicates: Boolean = false): Boolean = {
    require(!readOnly, "Cannot add header to readOnly HeaderStorage")
    val tableId: Int = tableNames.getOrAdd(header.tableName)
    val tablePlusId = calcTablePlusId(tableId, header.id)
    if (!allowDuplicates && contains(tablePlusId, header.hash)) false
    else {
      val tableIdHash: Long = calcTableIdHash(tablePlusId, header.hash)
      if (newTableIdHashes.add(tableIdHash)) hashCount += 1
      newTableIdOffsets.put(tablePlusId,
        newTableIdOffsets.get(tablePlusId) match {
          case null => List(header.offset)
          case offsets => header.offset :: offsets
        })
      if (header.timestamp < minTimestamp) minTimestamp = header.timestamp
      if (header.timestamp > maxTimestamp) maxTimestamp = header.timestamp
      count += 1
      true
    }
  }

  /**
   * Объединить накопленные оффсеты [[newTableIdOffsets]] со старыми [[tableIdOffsets]],
   * получив новый сортированный массив всех оффсетов.
   * Этот метод перезаписывает [[tableIdOffsets]] новым массивом, и обнуляет [[newTableIdOffsets]]
   */
  protected def mergeTableIdOffsets() {
    val result: LongByteBuffer = tableIdOffsets.enlargeCopy(count)
    var i = tableIdOffsets.length
    newTableIdOffsets.forEach(new IntObjConsumer[List[Int]] {
      override def accept(tablePlusId: Int, offsets: List[Int]): Unit = offsets.foreach {offset =>
        result(i) = calcTableIdOffset(tablePlusId, offset)
        i += 1
      }
    })
    result.sort()
    tableIdOffsets = result
    newTableIdOffsets.clear()
  }

  /**
   * Объединить накопленные хеши [[newTableIdHashes]] со старыми [[mergeTableIdHashes()]],
   * получив новый сортированный массив всех хешей.
   * Этот метод перезаписывает [[tableIdHashes]] новым массивом, и обнуляет [[newTableIdHashes]]
   */
  protected def mergeTableIdHashes() {
    var i = 0
    val result: LongByteBuffer = {
      i = tableIdHashes.length
      tableIdHashes.enlargeCopy(hashCount)
    }
    newTableIdHashes.forEach(new LongConsumer {
      override def accept(value: Long): Unit = {
        result(i) = value
        i += 1
      }
    })
    result.sort()
    tableIdHashes = result
    newTableIdHashes.clear()
  }

  def needSave: Boolean = _needSave || count != savedCount

  /**
   * Сохранить все хедеры, и, возможно, хеши.
   * Перед сохранением вызываются [[mergeTableIdOffsets()]] и [[mergeTableIdHashes()]],
   * что меняет внутренние структуры хранилища. После объединения это хранилище будет занимать
   * меньше памяти.
   *
   * @param rw Куда будет записаны хедеры
   * @param maybeHashesRw Если указан, то хеши будут записаны сюда. Этот параметр не следует указывать,
   *                      если хеши не были корректно загружены при создании класса. В таком случае
   *                      будет exception.
   */
  def save(rw: ReadWrite, maybeHashesRw: Option[ReadWrite]): Unit = {
    // write version
    rw.putInt(1)
    // write readOnly flag
    rw.putByte(if (readOnly) 1 else 0)
    // write timestamps
    rw.putLong(minTimestamp)
    rw.putLong(maxTimestamp)

    // merge tables
    mergeTableIdOffsets()
    mergeTableIdHashes()

    // write counts
    rw.putInt(tableIdOffsets.length)
    rw.putInt(tableIdHashes.length)

    // write TableNames
    locally {
      val values: ArrayBuffer[String] = tableNames.values
      rw.putInt(values.length)
      values.foreach(v => writeBytes(rw, v.getBytes(Charset)))
    }

    // write tableIdOffset array
    writeLongByteBuffer(rw, tableIdOffsets)

    rw.close()
    savedCount = count
    _needSave = false

    // write hashes
    maybeHashesRw.foreach(saveHashes)
  }

  /**
   * Сохранить хеши. Этот метод вызывает [[mergeTableIdHashes()]], что приводит к смене данных
   * внутри этого объекта.
   * Не следует вызываеть этот метод, если хеши не были корректно загружены при создании класса.
   * В таком случае будет exception.
   */
  protected[storage] def saveHashes(rw: ReadWrite): Unit = {
    require(hasValidHashes, "Cannot save inconsistent hashes. Hashes must be properly initialized before saving it.")
    rw.putInt(tableIdHashes.length)
    writeLongByteBuffer(rw, tableIdHashes)
    rw.close()
  }
}


class NewHeaderStorage extends HeaderStorage {
  override val tableNames: TableNames = new TableNames()
  override def hasValidHashes: Boolean = true
}


class ExistedHeaderStorage(read: ReadStream, readHashes: ReadStream) extends HeaderStorage {
  // read & check versions
  read.getInt match {
    case 1 => // ok
    case invalidVersion => sys.error("Invalid HeaderStorage version: " + invalidVersion)
  }
  // read readOnly flag
  readOnly = read.getByte != 0
  // read timestamps
  minTimestamp = read.getLong
  maxTimestamp = read.getLong
  require(minTimestamp == Long.MaxValue || minTimestamp <= maxTimestamp, "Invalid timestamps read")

  // read counts
  count = read.getInt
  hashCount = read.getInt
  savedCount = count

  // read TableNames
  override val tableNames: TableNames = {
    val tableCount = read.getInt
    val names: TableNames = new TableNames(tableCount)
    for (i <- 0 until tableCount) {
      names.getOrAdd(new String(readBytes(read), Charset))
    }
    names
  }

  // read tableIdOffset array
  tableIdOffsets = readLongByteBuffer(read, count)

  read.close()

  private var validHashes: Boolean = false

  // read tableIdHash array
  if (readHashes.available) {
    val gotHashCount = readHashes.getInt
    validHashes = gotHashCount == hashCount
    if (validHashes) tableIdHashes = readLongByteBuffer(readHashes, gotHashCount)
  }
  readHashes.close()

  override def hasValidHashes: Boolean = validHashes
}


/**
 * Легковесная реализация [[EssentialHeaderStorage]], которая читает только шапку хедеров,
 * но не сами хедеры.
 */
class EssentialHeaderStorageImpl(read: ReadStream) extends EssentialHeaderStorage {
  // read & check versions
  read.getInt match {
    case 1 => // ok
    case invalidVersion => sys.error("Invalid HeaderStorage version: " + invalidVersion)
  }
  // read readOnly flag
  override val isReadOnly: Boolean = read.getByte != 0
  // read timestamps
  override val getMinTimestamp: Long = read.getLong
  override val getMaxTimestamp: Long = read.getLong
  require(getMinTimestamp <= getMaxTimestamp, "Invalid timestamps read")
  // read counts
  override val getCount: Int = read.getInt
  override val getHashCount: Int = read.getInt
  read.close()
}



/**
 * Класс для хранения всех имён таблиц, используемых в [[HeaderStorage]].
 * Сопоставляет каждой таблице id, начиная с 0.
 * @param expectedSize Предполагаемое количество таблиц.
 */
class TableNames(expectedSize: Int = 16) {
  val values = new mutable.ArrayBuffer[String](expectedSize)
  val map: HashObjIntMap[String] = HashObjIntMaps.newUpdatableMap[String](expectedSize)

  /** Получить id по имени таблицы. Возвращает -1, если таблица не найдена */
  def get(tableName: String): Int = map.getOrDefault(tableName, -1)

  def getOrAdd(tableName: String): Int =
    map.getOrDefault(tableName, -1) match {
      case -1 =>
        val id = values.length
        map.put(tableName, id)
        values += tableName
        id
      case id => id
    }
}
