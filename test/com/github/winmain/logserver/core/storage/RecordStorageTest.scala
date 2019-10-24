package com.github.winmain.logserver.core.storage
import java.nio.ByteBuffer

import com.github.winmain.logserver.core.storage.Storage._
import org.slf4j.Logger
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.collection.mutable.ArrayBuffer

class RecordStorageTest extends Specification with Mockito {
  val opts: StorageOpts = new StorageOpts

  val rec1: Record = Record(123L, "table", RecordId(3), "abcdef".getBytes)
  val rec2: Record = Record(523L, "qweqwe", RecordId(0), "ppp".getBytes)
  val rec3: Record = Record(80L, "ros_bill.bill", RecordId(90), "some bytes".getBytes)

  "one record" in {
    val buf = ByteBuffer.allocate(8192)
    val rw = new ReadWriteBuffer(buf, emptyBuffer = true)

    // step1: write one record to storage
    val totalBytes = opts.recordStorageHeaderSize + 8 + 5 + 5 + 4 + 4 + 6
    var recOffset = 0
    locally {
      val ws: AppendableRecordStorage = new AppendableRecordStorage(rw)
      recOffset = ws.addRecord(rec1).get
      recOffset === opts.recordStorageHeaderSize
      val pos = rw.pos
      ws.close()

      rw.truncate(pos)
      pos === totalBytes
    }

    // step2: read record from storage
    locally {
      rw.seek(0)
      val rs = new ReadOnceRecordStorage(rw)
      rs.readRecord(recOffset) === rec1
      rs.headVersion === opts.recordStorageVersion
      rs.headMinTimestamp === 123L
      rs.headMaxTimestamp === 123L
      rs.headNeedSave === false
      rs.headRecordNum === 1
      rs.headTotalBytes === totalBytes
      rs.close()
    }
    success
  }


  "3 records" in {
    val buf = ByteBuffer.allocate(8192)
    val rw = new ReadWriteBuffer(buf, emptyBuffer = true)

    // step1: write 3 records to storage
    val totalBytes =
      opts.recordStorageHeaderSize + 8 + 5 + 5 + 4 + 4 + 6 +
        8 + 5 + 6 + 4 + 4 + 3 +
        8 + 5 + 13 + 4 + 4 + 10
    val recOffsets = Array.ofDim[Int](3)
    locally {
      val log = mock[Logger]
      log.warn(any) throws new RuntimeException("Must not call log.warn")
      log.warn(any, any[Throwable]) throws new RuntimeException("Must not call log.warn")
      val ws: AppendableRecordStorage = new AppendableRecordStorage(rw, log = log)
      recOffsets(0) = ws.addRecord(rec1).get
      recOffsets(1) = ws.addRecord(rec2).get
      recOffsets(2) = ws.addRecord(rec3).get
      recOffsets === Array(64, 96, 126)
      val pos = rw.pos
      ws.close()

      rw.truncate(pos)
      pos === totalBytes
    }

    // step2: read 3 records from storage
    locally {
      rw.seek(0)
      val rs = new ReadOnceRecordStorage(rw)
      rs.readRecord(recOffsets(0)) === rec1
      rs.readRecord(recOffsets(1)) === rec2
      rs.readRecord(recOffsets(2)) === rec3
      rs.headVersion === opts.recordStorageVersion
      rs.headMinTimestamp === 80L
      rs.headMaxTimestamp === 523L
      rs.headNeedSave === false
      rs.headRecordNum === 3
      rs.headTotalBytes === totalBytes
      rs.close()
    }
    success
  }


  "recovery 3 records" in {
    val buf = ByteBuffer.allocate(8192)
    val rw = new ReadWriteBuffer(buf, emptyBuffer = true)

    // step1: write 3 records to storage, not closing it
    val totalBytes =
      opts.recordStorageHeaderSize + 8 + 5 + 5 + 4 + 4 + 6 +
        8 + 5 + 6 + 4 + 4 + 3 +
        8 + 5 + 13 + 4 + 4 + 10
    val recOffsets = Array.ofDim[Int](3)
    locally {
      val log = mock[Logger]
      log.warn(any) throws new RuntimeException("Must not call log.warn")
      log.warn(any, any[Throwable]) throws new RuntimeException("Must not call log.warn")
      val ws: AppendableRecordStorage = new AppendableRecordStorage(rw, log = log)
      recOffsets(0) = ws.addRecord(rec1).get
      recOffsets(1) = ws.addRecord(rec2).get
      recOffsets(2) = ws.addRecord(rec3).get
      recOffsets === Array(64, 96, 126)
      val pos = rw.pos

      rw.truncate(pos)
      pos === totalBytes
    }

    // step2: validate old head
    locally {
      rw.seek(0)
      val rs = new ReadOnceRecordStorage(rw)
      rs.headVersion === opts.recordStorageVersion
      rs.headMinTimestamp === Long.MaxValue
      rs.headMaxTimestamp === 0L
      rs.headNeedSave === false
      rs.headRecordNum === 0
      rs.headTotalBytes === 64
    }

    // step3: recover records
    locally {
      rw.seek(0)
      val log = mock[Logger]
      var recovered = ArrayBuffer[Header]()
      def recoverReceiver(header: Header): Unit = recovered += header
      val ws: AppendableRecordStorage = new AppendableRecordStorage(rw, log = log, onRecoverReceiver = recoverReceiver)
      there was one(log).warn(anyString)
      recovered === Vector(
        Header(123L, 64, rec1.calcHash, "table", RecordId(3)),
        Header(523L, 96, rec2.calcHash, "qweqwe", RecordId(0)),
        Header(80L, 126, rec3.calcHash, "ros_bill.bill", RecordId(90)))
      ws.readRecord(recOffsets(0)) === rec1
      ws.readRecord(recOffsets(1)) === rec2
      ws.readRecord(recOffsets(2)) === rec3
      ws.headVersion === opts.recordStorageVersion
      ws.headMinTimestamp === 80L
      ws.headMaxTimestamp === 523L
      ws.headNeedSave === true
      ws.headRecordNum === 3
      ws.headTotalBytes === totalBytes
      ws.close()
    }
    success
  }


  "recovery 2 records + crashed one" in {
    val buf = ByteBuffer.allocate(8192)
    val rw = new ReadWriteBuffer(buf, emptyBuffer = true)

    // step1: write 3 records to storage
    locally {
      val log = mock[Logger]
      val ws: AppendableRecordStorage = new AppendableRecordStorage(rw, log = log)
      ws.addRecord(rec1)
      ws.addRecord(rec2)
      ws.addRecord(rec3)
      ws.close()
      no(log).warn(anyString)
      no(log).warn(anyString, any[Throwable])
    }

    // step2: crush last record & try to recover
    locally {
      buf.limit(buf.limit() - 10)
      rw.seek(0)
      val log = mock[Logger]
      val hs: NewHeaderStorage = new RecoveryRecordStorage(rw, () => rw, opts = opts, log = log).toHeaderStorage
      hs.getCount === 2
      hs.contains(rec1.tableName, rec1.id, rec1.calcHash) === true
      hs.contains(rec2.tableName, rec2.id, rec2.calcHash) === true
      hs.contains(rec3.tableName, rec3.id, rec3.calcHash) === false
      no(log).warn(anyString)
      no(log).warn(anyString, any[Throwable])
    }
    success
  }
}
