package com.github.winmain.logserver.core

object UInt29 {

  /** Minimum required number of bytes to represent given int using UInt29 format */
  def size(ref: Int): Int =
    if (ref < 0x80) { // 0x00000000 - 0x0000007F : 0xxxxxxx
       1
    } else if (ref < 0x4000) { // 0x00000080 - 0x00003FFF : 1xxxxxxx 0xxxxxxx
      2
    } else if (ref < 0x200000) { // 0x00004000 - 0x001FFFFF : 1xxxxxxx 1xxxxxxx 0xxxxxxx
      3
    } else if (ref < 0x40000000) { // 0x00200000 - 0x3FFFFFFF : 1xxxxxxx 1xxxxxxx 1xxxxxxx xxxxxxxx
      4
    } else { // 0x40000000 - 0xFFFFFFFF : throw range exception
      throw new IllegalArgumentException("Integer out of range: " + ref)
    }

}

trait UInt29Reader[A] {

  def readByte(a: A): Byte

  def readUInt29(in: A): Int = {
    var value = 0

    // Each byte must be treated as unsigned
    var b = readByte(in) & 0xFF

    if (b < 128) return b

    value = (b & 0x7F) << 7;
    b = readByte(in) & 0xFF;

    if (b < 128) return value | b

    value = (value | (b & 0x7F)) << 7;
    b = readByte(in) & 0xFF

    if (b < 128) return value | b

    value = (value | (b & 0x7F)) << 8
    b = readByte(in) & 0xFF

    value | b
  }

}

object UInt29Reader {

  def apply[A](implicit reader: UInt29Reader[A]): UInt29Reader[A] = reader

  implicit def toUInt29ReaderOps[A](a: A)(implicit reader: UInt29Reader[A]): UInt29ReaderOps[A] =
    new UInt29ReaderOps(a)

  final class UInt29ReaderOps[A : UInt29Reader](a: A) {
    def readUInt29(): Int = UInt29Reader[A].readUInt29(a)
  }

}

trait UInt29Writer[A] {

  def writeByte(a: A, byte: Byte): Unit

  def writeUInt29(a: A, ref: Int): Unit = {
    // Represent smaller integers with fewer bytes using the most
    // significant bit of each byte. The worst case uses 32-bits
    // to represent a 29-bit number, which is what we would have
    // done with no compression.

    // 0x00000000 - 0x0000007F : 0xxxxxxx
    // 0x00000080 - 0x00003FFF : 1xxxxxxx 0xxxxxxx
    // 0x00004000 - 0x001FFFFF : 1xxxxxxx 1xxxxxxx 0xxxxxxx
    // 0x00200000 - 0x3FFFFFFF : 1xxxxxxx 1xxxxxxx 1xxxxxxx xxxxxxxx
    // 0x40000000 - 0xFFFFFFFF : throw range exception
    if (ref < 0x80) { // 0x00000000 - 0x0000007F : 0xxxxxxx
      writeByte(a, ref.toByte)
    } else if (ref < 0x4000) { // 0x00000080 - 0x00003FFF : 1xxxxxxx 0xxxxxxx
      writeByte(a, (((ref >> 7) & 0x7F) | 0x80).toByte)
      writeByte(a, (ref & 0x7F).toByte)
    } else if (ref < 0x200000) { // 0x00004000 - 0x001FFFFF : 1xxxxxxx 1xxxxxxx 0xxxxxxx
      writeByte(a, (((ref >> 14) & 0x7F) | 0x80).toByte)
      writeByte(a, (((ref >> 7) & 0x7F) | 0x80).toByte)
      writeByte(a, (ref & 0x7F).toByte)
    } else if (ref < 0x40000000) { // 0x00200000 - 0x3FFFFFFF : 1xxxxxxx 1xxxxxxx 1xxxxxxx xxxxxxxx
      writeByte(a, (((ref >> 22) & 0x7F) | 0x80).toByte)
      writeByte(a, (((ref >> 15) & 0x7F) | 0x80).toByte)
      writeByte(a, (((ref >> 8) & 0x7F) | 0x80).toByte)
      writeByte(a, (ref & 0xFF).toByte)
    } else { // 0x40000000 - 0xFFFFFFFF : throw range exception
      throw new IllegalArgumentException("Integer out of range: " + ref)
    }
  }

}

object UInt29Writer {

  def apply[A](implicit writer: UInt29Writer[A]): UInt29Writer[A] = writer

  implicit def toUInt29WriterOps[A](a: A)(implicit writer: UInt29Writer[A]): UInt29WriterOps[A] =
    new UInt29WriterOps(a)

  final class UInt29WriterOps[A : UInt29Writer](a: A) {
    def writeUInt29(ref: Int): Unit = UInt29Writer[A].writeUInt29(a, ref)
  }

}