package com.github.winmain.logserver.core.storage
import java.nio.ByteBuffer

import com.github.winmain.logserver.core.storage.Storage.{Header, RecordId}
import org.specs2.mutable.Specification

class HeaderStorageTest extends Specification {
  "big test" in {
    val buf = ByteBuffer.allocate(8192)
    val rw = new ReadWriteBuffer(buf, emptyBuffer = true)

    val hashBuf = ByteBuffer.allocate(8192)
    val hashRw = new ReadWriteBuffer(hashBuf, emptyBuffer = true)

    // step1: fill new storage
    locally {
      val hs: NewHeaderStorage = new NewHeaderStorage
      hs.needSave === false
      hs.isReadOnly === false
      hs.getCount === 0
      hs.add(Header(50L, 150, 555999, "user", RecordId(1))) === true
      hs.getCount === 1
      hs.add(Header(78L, 160, 1234, "user", RecordId(1))) === true
      hs.getCount === 2
      hs.add(Header(34L, 170, -62438, "user", RecordId(5))) === true
      hs.getCount === 3
      hs.add(Header(25L, 180, 1234, "account", RecordId(1))) === true
      hs.add(Header(71L, 170, 1234, "user", RecordId(1))) === false // duplicate

      hs.needSave === true
      hs.getCount === 4
      hs.getMinTimestamp === 25L
      hs.getMaxTimestamp === 78L
      hs.contains("user", RecordId(1), 555999) === true
      hs.contains("user", RecordId(1), 1234) === true
      hs.contains("user", RecordId(1), 1235) === false
      hs.contains("user", RecordId(0), 1234) === false
      hs.contains("custom-table", RecordId(1), 1234) === false
      hs.contains("account", RecordId(1), 1234) === true
      hs.contains("account", RecordId(1), 555999) === false
      hs.getOffsets("user", RecordId(1)).toSet === Set(150, 160)
      hs.getOffsets("user", RecordId(0)) === Nil
      hs.getOffsets("user", RecordId(5)).toSet === Set(170)
      hs.getOffsets("account", RecordId(1)).toSet === Set(180)
      hs.getOffsets("another table", RecordId(1)) === Nil

      hs.save(rw, Some(hashRw))
      hs.needSave === false
    }

    locally {
      // step2: read storage
      buf.rewind()
      hashBuf.rewind()
      val hs: ExistedHeaderStorage = new ExistedHeaderStorage(rw, hashRw)
      // С копипастой проверок ниже проще - в случше ошибки выводится только номер строки, стектрейса нет.
      hs.getCount === 4
      hs.contains("user", RecordId(1), 555999) === true
      hs.contains("user", RecordId(1), 1234) === true
      hs.contains("user", RecordId(1), 1235) === false
      hs.contains("user", RecordId(0), 1234) === false
      hs.contains("custom-table", RecordId(1), 1234) === false
      hs.contains("account", RecordId(1), 1234) === true
      hs.contains("account", RecordId(1), 555999) === false
      hs.getOffsets("user", RecordId(1)).toSet === Set(150, 160)
      hs.getOffsets("user", RecordId(0)) === Nil
      hs.getOffsets("user", RecordId(5)).toSet === Set(170)
      hs.getOffsets("account", RecordId(1)).toSet === Set(180)
      hs.getOffsets("another table", RecordId(1)) === Nil
      hs.needSave === false

      // step3: modify storage
      hs.add(Header(88L, 150, 555999, "user", RecordId(1))) === false
      hs.add(Header(43L, 190, 789, "user", RecordId(1))) === true
      hs.add(Header(29L, 195, 99, "user", RecordId(2))) === true
      hs.add(Header(90L, 200, 35, "account", RecordId(1))) === true
      hs.add(Header(55L, 201, 1234, "account", RecordId(1))) === false
      hs.add(Header(95L, 202, 35, "account", RecordId(1))) === false
      hs.setReadOnly(true)
      hs.add(Header(95L, 202, 35, "account", RecordId(1))) must throwA("readOnly")

      hs.contains("user", RecordId(1), 1234) === true
      hs.contains("user", RecordId(1), 789) === true
      hs.contains("user", RecordId(1), 35) === false
      hs.contains("account", RecordId(1), 35) === true
      hs.getOffsets("user", RecordId(1)).toSet === Set(150, 160, 190)
      hs.getOffsets("user", RecordId(2)).toSet === Set(195)
      hs.getOffsets("account", RecordId(1)).toSet === Set(180, 200)
      hs.needSave === true

      buf.limit(0)
      hashBuf.limit(0)
      hs.save(rw, Some(hashRw))
      hs.needSave === false
    }

    locally {
      // step4: re-read updated storage
      buf.rewind()
      hashBuf.rewind()
      val hs: ExistedHeaderStorage = new ExistedHeaderStorage(rw, hashRw)
      hs.getCount === 7
      hs.isReadOnly === true
      hs.getMinTimestamp === 25L
      hs.getMaxTimestamp === 90L
      hs.contains("user", RecordId(1), 789) === true
      hs.contains("user", RecordId(1), 1234) === true
      hs.contains("account", RecordId(1), 1234) === true
      hs.contains("account", RecordId(1), 35) === true
      hs.getOffsets("user", RecordId(1)).toSet === Set(150, 160, 190)
      hs.getOffsets("user", RecordId(2)).toSet === Set(195)
      hs.getOffsets("user", RecordId(5)).toSet === Set(170)
      hs.getOffsets("account", RecordId(1)).toSet === Set(180, 200)
    }
    success
  }


  "test no hashes" in {
    val buf = ByteBuffer.allocate(8192)
    val rw = new ReadWriteBuffer(buf, emptyBuffer = true)

    // step1: fill new storage
    locally {
      val hs: NewHeaderStorage = new NewHeaderStorage
      hs.add(Header(5L, 150, 555999, "user", RecordId(1)))
      hs.add(Header(6L, 160, 1234, "user", RecordId(1)))
      hs.add(Header(7L, 170, -62438, "user", RecordId(5)))

      hs.save(rw, None)
    }

    // step2: load storage
    locally {
      buf.rewind()
      val emptyHashBuf = ByteBuffer.allocate(0)
      val emptyHashRw = new ReadWriteBuffer(emptyHashBuf)
      val hs: ExistedHeaderStorage = new ExistedHeaderStorage(rw, emptyHashRw)
      hs.contains("user", RecordId(0), 0) must throwA("No hashes loaded")
      hs.contains("user", RecordId(1), 555999) must throwA("No hashes loaded")
      hs.getOffsets("user", RecordId(1)).toSet === Set(150, 160)

      hs.save(rw, Some(rw)) must throwA("Cannot save inconsistent hashes")
    }
    success
  }


  "test invalid hashes" in {
    val buf = ByteBuffer.allocate(8192)
    val rw = new ReadWriteBuffer(buf, emptyBuffer = true)

    val hashBuf = ByteBuffer.allocate(8192)
    val hashRw = new ReadWriteBuffer(hashBuf, emptyBuffer = true)

    // step1: fill new storage
    locally {
      val hs: NewHeaderStorage = new NewHeaderStorage
      hs.add(Header(5L, 150, 555999, "user", RecordId(1)))
      hs.saveHashes(hashRw)

      hs.add(Header(6L, 160, 1234, "user", RecordId(1)))
      hs.add(Header(7L, 170, -62438, "user", RecordId(5)))
      hs.save(rw, None)
    }

    // step2: load storage
    locally {
      buf.rewind()
      hashBuf.rewind()
      val hs: ExistedHeaderStorage = new ExistedHeaderStorage(rw, hashRw)
      hs.contains("user", RecordId(0), 0) must throwA("No hashes loaded")
      hs.contains("user", RecordId(1), 555999) must throwA("No hashes loaded")
      hs.getOffsets("user", RecordId(1)).toSet === Set(150, 160)

      hs.save(rw, Some(rw)) must throwA("Cannot save inconsistent hashes")
    }
    success
  }
}
