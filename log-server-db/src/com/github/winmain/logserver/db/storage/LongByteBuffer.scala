package com.github.winmain.logserver.db.storage

import java.nio.{ByteBuffer, LongBuffer}

class LongByteBuffer(val bb: ByteBuffer) {
  def this(capacity: Int) = this(ByteBuffer.allocate(capacity * 8))

  val lb: LongBuffer = {bb.clear(); bb.asLongBuffer()}

  val length: Int = lb.capacity()
  def apply(idx: Int): Long = lb.get(idx)
  def update(idx: Int, v: Long): Unit = lb.put(idx, v)

  def binarySearch(key: Long): Int = {
    var low: Int = 0
    var high: Int = length - 1

    while (low <= high) {
      val mid: Int = (low + high) >>> 1
      val midVal: Long = apply(mid)
      if (midVal < key) low = mid + 1
      else if (midVal > key) high = mid - 1
      else return mid // key found
    }
    -(low + 1) // key not found.
  }

  def enlargeCopy(newSize: Int): LongByteBuffer = {
    val newBb = ByteBuffer.allocate(newSize * 8)
    bb.rewind()
    newBb.put(bb)
    new LongByteBuffer(newBb)
  }

  def copyTo(srcPos: Int, dest: LongByteBuffer, destPos: Int, length: Int): Unit = {
    val src = bb.slice()
    src.position(srcPos)
    src.limit(math.max(src.capacity(), srcPos + length))
    val dst = dest.bb.slice()
    dst.position(destPos)
    dst.limit(math.max(dst.capacity(), destPos + length))
    dst.put(src)
  }

  def sort(): Unit = {
    LongByteBufferUtils.sort(this, 0, length - 1, null, 0, 0)
  }
}

object EmptyLongByteBuffer extends LongByteBuffer(ByteBuffer.allocate(0))
