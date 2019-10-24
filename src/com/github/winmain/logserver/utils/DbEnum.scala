package com.github.winmain.logserver.utils
import scala.collection.mutable

/**
 * Реализация Enumerable с ключевым полем типа int.
 * Внутреннее хранилище основано на ArrayBuffer, поэтому ключи не должны быть с большими значениями.
 */
abstract class DbEnum {
  type V <: Cls

  private[utils] val valueMap = new mutable.ArrayBuffer[V](initialSize)
  private[utils] var _values = Vector[V]()

  protected def initialSize = 16

  def getValue(id: Int): Option[V] = {
    try {
      Option(valueMap(id))
    } catch {case e: IndexOutOfBoundsException => None}
  }
  def getByIndex(index: Int): Option[V] = if (isValidIndex(index)) Some(values(index)) else None

  def values: Vector[V] = _values

  def isValidIndex(index: Int): Boolean = index >= 0 && index < _values.size


  abstract class Cls protected(id: Int) {self: V =>
    locally {
      if (id < 0) sys.error("Invalid DbEnum id:" + id)
      if (id < valueMap.length && valueMap(id) != null) sys.error("Duplicate DbEnum with id:" + id)
      val e = this.asInstanceOf[V]
      while (valueMap.length < id) valueMap += null.asInstanceOf[V]
      if (id == valueMap.length) valueMap += e else valueMap(id) = e
      _values = _values :+ e
    }

    def getId: Int = id
    def in(values: Set[V]): Boolean = values.contains(this)
    def in(values: Seq[V]): Boolean = values.contains(this)

    val index: Int = _values.size - 1
  }
}
