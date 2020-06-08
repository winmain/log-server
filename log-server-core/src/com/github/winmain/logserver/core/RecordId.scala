package com.github.winmain.logserver.core

import java.util

import com.fasterxml.jackson.annotation.JsonValue

import scala.util.Try

sealed trait RecordId {

  import RecordId._

  /** Хэш идентификатора */
  def hash: Int

  /** Размер идентификатора в байтах при записи в базу */
  def length: Int

  /** Как этот идентификатор будет выглядеть при сериализации в JSON */
  @JsonValue
  def jsonValue: Any =
    this match {
      case i: IntRecordId    => i.value
      case s: StringRecordId => new String(s.value, LogServer.Charset)
      case EmptyRecordId     => null
    }

  override def equals(other: Any): Boolean = {
    this match {
      case i: IntRecordId =>
        other.isInstanceOf[IntRecordId] && other
          .asInstanceOf[IntRecordId]
          .value == i.value

      case s: StringRecordId =>
        other.isInstanceOf[StringRecordId] && util.Arrays
          .equals(other.asInstanceOf[StringRecordId].value, s.value)

      case EmptyRecordId =>
        this == EmptyRecordId
    }
  }
}

object RecordId {
  val EmptyIdMarker: Byte = 0
  val IntIdMarker: Byte = 1
  val StringIdMarker: Byte = 2

  def apply(i: Int): RecordId =
    new IntRecordId(i)

  def apply(s: String): RecordId =
    new StringRecordId(s.getBytes(LogServer.Charset))

  def str(bytes: Array[Byte]): RecordId =
    new StringRecordId(bytes)

  def empty: RecordId = EmptyRecordId

  def parse(s: String): RecordId =
    Try(s.toInt).map(RecordId(_)).getOrElse(RecordId(s))

  class IntRecordId(val value: Int) extends RecordId {
    def hash: Int = value
    def length: Int = 5
    override def toString: String = value.toString
  }

  class StringRecordId(val value: Array[Byte]) extends RecordId {
    def hash: Int = value.foldLeft(0)(_ * 31 + _)
    def length: Int = 1 + UInt29.size(value.length) + value.length
    override def toString: String = new String(value, LogServer.Charset)
  }

  case object EmptyRecordId extends RecordId {
    override def hash: Int = 0
    override def length: Int = 1
  }

}
