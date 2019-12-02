package com.github.winmain.logserver.db.reader
import java.util.concurrent.{LinkedBlockingQueue, SynchronousQueue, TimeUnit}

import com.github.winmain.logserver.core.RecordId
import com.github.winmain.logserver.db.SourceLogRecord
import com.github.winmain.logserver.db.storage.AppendableBigStorage
import org.slf4j.Logger

import scala.collection.mutable

class MemoryWiseLogWrapper(reader: LogReader, maxMemory: Long = 10000000L, bufferQueueCapacity: Int = 2) {
  private val maxMemoryPerThread = maxMemory / (bufferQueueCapacity + 2)

  class It extends Iterator[Iterable[SourceLogRecord]] {
    private val logQueue: SynchronousQueue[SourceLogRecord] = new SynchronousQueue(true)
    private val bufferQueue: LinkedBlockingQueue[Iterable[SourceLogRecord]] = new LinkedBlockingQueue(bufferQueueCapacity)

    val readThread: Thread = new Thread("MemoryWise:readThread") {
      override def run(): Unit = {
        try reader.readLogs(logQueue)
        catch {case e: InterruptedException =>}
        if (bufferThread.isAlive) logQueue.offer(new SourceLogRecordFinish, 100, TimeUnit.MILLISECONDS)
        //// println("ReadThread stopped") ////////
      }
    }
    readThread.start()

    val bufferThread: Thread = new Thread("MemoryWise:bufferThread") {
      override def run(): Unit = {
        try {
          while (readThread.isAlive) {
            val records = mutable.Buffer[SourceLogRecord]()
            var occupiedMemory = 0L
            while (readThread.isAlive && occupiedMemory < maxMemoryPerThread) {
              val record: SourceLogRecord = logQueue.poll(100, TimeUnit.MILLISECONDS)
              if (record == null) {
                if (!readThread.isAlive) sys.exit(-1) // exception thrown in readThread
              } else if (record.isInstanceOf[SourceLogRecordFinish]) {
                readThread.join()
              } else {
                occupiedMemory += record.occupiedMemory
                records += record
              }
            }
            //// print("Putting bufferQueue records:" + records.size)
            bufferQueue.put(records)
            //// println("    ok")
          }
          bufferQueue.put(Seq.empty)
        } catch {case e: InterruptedException =>}
        //// println("BufferThread stopped") /////////
      }
    }
    bufferThread.start()

    private var nextValue: Iterable[SourceLogRecord] = null

    private def checkNextValue(): Unit = {
      if (nextValue == null) nextValue = bufferQueue.take()
    }

    override def hasNext: Boolean = {
      checkNextValue()
      nextValue.nonEmpty
    }
    override def next(): Iterable[SourceLogRecord] = {
      checkNextValue()
      val ret = nextValue
      if (nextValue.nonEmpty) nextValue = null
      ret
    }

    def stop(): Unit = {
      if (readThread.isAlive) readThread.interrupt()
      if (bufferThread.isAlive) bufferThread.interrupt()
    }
  }

  // Маркерный SourceLogRecord, показывающий что данных больше нет
  class SourceLogRecordFinish extends SourceLogRecord {
    override def tableName: String = null
    override def occupiedMemory: Int = 0
    override def logBytesUTF8: Array[Byte] = null
    override def id: RecordId = RecordId.empty
    override def timestamp: Long = 0L
  }


  def withIterator[T](block: Iterator[Iterable[SourceLogRecord]] => T): Unit = {
    val it: It = new It
    try block(it)
    finally {
      it.stop()
    }
  }

  def addRecords(big: AppendableBigStorage, log: Logger): Unit = withIterator {it =>
    for (logGroup <- it) {
      var added = 0
      var duplicates = 0
      logGroup.foreach {sourceLogRecord =>
        if (big.addRecord(sourceLogRecord.toStorageRecord)) added += 1
        else duplicates += 1
      }
      log.info(
        "Writed logGroup size:" + logGroup.size +
          (if (duplicates == 0) ", added:" + added
          else ", added:" + added + ", duplicates:" + duplicates)
      )
    }
  }
}
