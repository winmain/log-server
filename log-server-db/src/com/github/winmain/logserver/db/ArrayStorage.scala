package com.github.winmain.logserver.db

import java.nio.ByteBuffer

import com.github.winmain.logserver.db.utils.{BufferUtils, DbEnum}

/**
 * Набор классов для эффективного хранения массивов Long (значения можно хранить в Int)
 * и String (значения могут быть числовыми, поэтому возможно хранение в Int, Long)
 */
object ArrayStorage {
  val MaxSize = 1024
  require(MaxSize.toShort == MaxSize, "Cannot serialize/deserialize ArrayStorages: MaxSize too big")

  def newLongStorage(value: Long): ArrayStorage[Long] = value match {
    case 0L => new EmptyStorageLong
    case v if v.toInt == v => new IntArrayStorageLong
    case _ => new LongArrayStorage
  }

  def newStringStorage(value: String): ArrayStorage[String] = {
    if (value.isEmpty) new EmptyStorageString
    else {
      val longValue: Long =
        try value.toLong
        catch {case e: Throwable => return new StringArrayStorage}
      if (longValue.toString != value) new StringArrayStorage
      else if (longValue.toInt == longValue && longValue != Int.MinValue) new IntArrayStorageString
      else if (longValue != Long.MinValue) new LongArrayStorageString
      else new StringArrayStorage
    }
  }

  def deserializeFrom(buf: ByteBuffer): ArrayStorage[_] = {
    val id: Short = buf.getShort
    val factory = StorageIdFactory.getValue(id).getOrElse(sys.error("Unable to get StorageIdFactory for id:" + id))
    val storage: ArrayStorage[_] = factory.create
    storage.deserializeFromNoId(buf)
    storage
  }
}

// ------------------------------- StorageIdFacoty for serializing -------------------------------

object StorageIdFactory extends DbEnum {
  override type V = StorageIdFactory

  val EmptyStorageLong = new V(1, new EmptyStorageLong)
  val LongArrayStorage = new V(2, new LongArrayStorage)
  val IntArrayStorageLong = new V(3, new IntArrayStorageLong)
  val EmptyStorageString = new V(4, new EmptyStorageString)
  val StringArrayStorage = new V(5, new StringArrayStorage)
  val IntArrayStorageString = new V(6, new IntArrayStorageString)
  val LongArrayStorageString = new V(7, new LongArrayStorageString)
}
class StorageIdFactory private(val id: Short, createFn: => ArrayStorage[_]) extends StorageIdFactory.Cls(id) {
  def create: ArrayStorage[_] = createFn
}

// ------------------------------- Abstract ArrayStorage -------------------------------

abstract class ArrayStorage[@specialized(Long) T] {
  protected var sz = 0
  def storageIdFactory: StorageIdFactory
  def maxSize: Int = ArrayStorage.MaxSize
  def size: Int = sz

  /** Добавить новое значение в хранилище.
    * Возвращает None, если старое хранилище не надо менять, либо Some(storage), если хранилище
    * требуется заменить на новое, потому что поступили данные бОльшего размера.
    */
  def add(value: T): Option[ArrayStorage[T]] = {
    if (sz >= maxSize) sys.error("Cannot add offset element: too many elements in this storage (" + sz + ")")
    if (checkAndSetValue(value)) {
      sz += 1
      None
    } else {
      val storage: ArrayStorage[T] = newStorage(value)
      require(storage.getClass != getClass, "NewStorage for value " + value + " returns same class " + getClass)
      var idx = 0
      while (idx < sz) {
        storage.add(get0(idx))
        idx += 1
      }
      storage.add(value)
      Some(storage)
    }
  }
  def apply(idx: Int): T = {
    if (idx < 0 || idx >= size) throw new IllegalArgumentException("Index out of bounds: " + idx)
    get0(idx)
  }

  def serializeTo(buf: ByteBuffer): Unit = {
    require(sz.toShort == sz, "Size is not short")
    buf.putShort(storageIdFactory.id)
    buf.putShort(sz.toShort)
    serializeData(buf)
  }

  def deserializeFromNoId(buf: ByteBuffer): Unit = {
    sz = buf.getShort
    require(sz >= 0 && sz <= ArrayStorage.MaxSize, "Invalid size: " + sz)
    deserializeData(buf)
  }

  protected def newStorage(value: T): ArrayStorage[T]
  protected def checkAndSetValue(value: T): Boolean
  protected def get0(idx: Int): T
  protected def serializeData(buf: ByteBuffer): Unit
  protected def deserializeData(buf: ByteBuffer): Unit
}

// ------------------------------- Long storages -------------------------------

class EmptyStorageLong extends ArrayStorage[Long] {
  override def storageIdFactory: StorageIdFactory = StorageIdFactory.EmptyStorageLong
  override protected def newStorage(value: Long): ArrayStorage[Long] = ArrayStorage.newLongStorage(value)
  override protected def checkAndSetValue(value: Long): Boolean = value == 0L
  override def get0(idx: Int): Long = 0L
  override protected def serializeData(buf: ByteBuffer): Unit = {}
  override protected def deserializeData(buf: ByteBuffer): Unit = {}
}

class IntArrayStorageLong extends ArrayStorage[Long] {
  override def storageIdFactory: StorageIdFactory = StorageIdFactory.IntArrayStorageLong
  private val values: Array[Int] = new Array[Int](maxSize)
  override protected def newStorage(value: Long): ArrayStorage[Long] = ArrayStorage.newLongStorage(value)
  override protected def checkAndSetValue(value: Long): Boolean = {
    if (value.toInt == value) {
      values(sz) = value.toInt
      true
    } else false
  }
  override def get0(idx: Int): Long = values(idx)
  override protected def serializeData(buf: ByteBuffer): Unit = {
    buf.asIntBuffer().put(values, 0, size)
    buf.position(buf.position() + size * 4)
  }
  override protected def deserializeData(buf: ByteBuffer): Unit = {
    buf.asIntBuffer().get(values, 0, size)
    buf.position(buf.position() + size * 4)
  }
}

class LongArrayStorage extends ArrayStorage[Long] {
  override def storageIdFactory: StorageIdFactory = StorageIdFactory.LongArrayStorage
  private val values: Array[Long] = new Array[Long](maxSize)
  override protected def newStorage(value: Long): ArrayStorage[Long] = ArrayStorage.newLongStorage(value)
  override protected def checkAndSetValue(value: Long): Boolean = {
    values(sz) = value
    true
  }
  override def get0(idx: Int): Long = values(idx)
  override protected def serializeData(buf: ByteBuffer): Unit = {
    buf.asLongBuffer().put(values, 0, size)
    buf.position(buf.position() + size * 8)
  }
  override protected def deserializeData(buf: ByteBuffer): Unit = {
    buf.asLongBuffer().get(values, 0, size)
    buf.position(buf.position() + size * 8)
  }
}

// ------------------------------- String storages -------------------------------

class EmptyStorageString extends ArrayStorage[String] {
  override def storageIdFactory: StorageIdFactory = StorageIdFactory.EmptyStorageString
  override protected def newStorage(value: String): ArrayStorage[String] = ArrayStorage.newStringStorage(value)
  override protected def checkAndSetValue(value: String): Boolean = value.isEmpty
  override def get0(idx: Int): String = ""
  override protected def serializeData(buf: ByteBuffer): Unit = {}
  override protected def deserializeData(buf: ByteBuffer): Unit = {}
}

class IntArrayStorageString extends ArrayStorage[String] {
  override def storageIdFactory: StorageIdFactory = StorageIdFactory.IntArrayStorageString
  private val values: Array[Int] = new Array[Int](maxSize)
  override protected def newStorage(value: String): ArrayStorage[String] = ArrayStorage.newStringStorage(value)
  override protected def checkAndSetValue(value: String): Boolean = {
    val intValue: Int =
      if (value.isEmpty) Int.MinValue
      else {
        val v: Int =
          try value.toInt
          catch {case e: Throwable => return false}
        if (v == Int.MinValue) return false // Int.MinValue зарезервирован под пустую строку, поэтому он является недопустимым значением в строке
        if (v.toString != value) return false
        v
      }
    values(sz) = intValue
    true
  }
  override def get0(idx: Int): String = {
    values(idx) match {
      case Int.MinValue => ""
      case v => v.toString
    }
  }
  override protected def serializeData(buf: ByteBuffer): Unit = {
    buf.asIntBuffer().put(values, 0, size)
    buf.position(buf.position() + size * 4)
  }
  override protected def deserializeData(buf: ByteBuffer): Unit = {
    buf.asIntBuffer().get(values, 0, size)
    buf.position(buf.position() + size * 4)
  }
}

class LongArrayStorageString extends ArrayStorage[String] {
  override def storageIdFactory: StorageIdFactory = StorageIdFactory.LongArrayStorageString
  private val values: Array[Long] = new Array[Long](maxSize)
  override protected def newStorage(value: String): ArrayStorage[String] = ArrayStorage.newStringStorage(value)
  override protected def checkAndSetValue(value: String): Boolean = {
    val longValue: Long =
      if (value.isEmpty) Long.MinValue
      else {
        val v: Long =
          try value.toLong
          catch {case e: Throwable => return false}
        if (v == Long.MinValue) return false // Long.MinValue зарезервирован под пустую строку, поэтому он является недопустимым значением в строке
        if (v.toString != value) return false
        v
      }
    values(sz) = longValue
    true
  }
  override def get0(idx: Int): String = {
    values(idx) match {
      case Long.MinValue => ""
      case v => v.toString
    }
  }
  override protected def serializeData(buf: ByteBuffer): Unit = {
    buf.asLongBuffer().put(values, 0, size)
    buf.position(buf.position() + size * 8)
  }
  override protected def deserializeData(buf: ByteBuffer): Unit = {
    buf.asLongBuffer().get(values, 0, size)
    buf.position(buf.position() + size * 8)
  }
}

class StringArrayStorage extends ArrayStorage[String] {
  override def storageIdFactory: StorageIdFactory = StorageIdFactory.StringArrayStorage
  private val values: Array[String] = new Array[String](maxSize)
  override protected def newStorage(value: String): ArrayStorage[String] = ArrayStorage.newStringStorage(value)
  override protected def checkAndSetValue(value: String): Boolean = {
    require(value != null, "Value cannot be null")
    values(sz) = value
    true
  }
  override def get0(idx: Int): String = values(idx)
  override protected def serializeData(buf: ByteBuffer): Unit = {
    var i = 0
    while (i < sz) {
      BufferUtils.putString(buf, values(i))
      i += 1
    }
  }
  override protected def deserializeData(buf: ByteBuffer): Unit = {
    var i = 0
    while (i < sz) {
      values(i) = BufferUtils.getString(buf)
      i += 1
    }
  }
}
