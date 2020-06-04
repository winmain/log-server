package com.github.winmain.logserver.db.storage

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import com.github.winmain.logserver.db.utils.{FileUtils, Str}
import org.slf4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.Seq
import scala.util.Try

// ------------------------------- Directory traits -------------------------------

trait Directory {
  def infos: Seq[StorageInfo]
  def addNewInfo(): StorageInfo
  def gzipInfo(info: StorageInfo): StorageInfo

  def locked: Boolean
  def canLock: Boolean
  def lock(log: Logger, wait: (Int, TimeUnit)): Unit
  def unlock(): Unit
}

trait StorageInfo {
  def gzipped: Boolean
  def recordStoragePath: Path

  def recordReadStream: ReadStream
  def headerReadStream: ReadStream
  def hashReadStream: ReadStream

  def recordReadWrite: ReadWrite
  def headerReadWrite: ReadWrite
  def hashReadWrite: ReadWrite

  // Этот метод нужно переопределить, потому что он используется для составления строки в логах
  def toString: String
  def name: String
}

// ------------------------------- Fake classes -------------------------------

class FakeDirectory(bufferSize: Int = 4096) extends Directory {
  var infos: mutable.Buffer[FakeStorageInfo] = new ArrayBuffer()
  override def addNewInfo(): FakeStorageInfo = {
    val info: FakeStorageInfo = new FakeStorageInfo(infos.length, bufferSize)
    infos += info
    info
  }
  override def gzipInfo(info: StorageInfo): StorageInfo = info

  var locked: Boolean = false
  override def canLock: Boolean = !locked
  override def lock(log: Logger, wait: (Int, TimeUnit)): Unit = {
    require(!locked, "Cannot lock already locked Directory")
    locked = true
  }
  override def unlock(): Unit = locked = false
}

class FakeStorageInfo(idx: Int, bufferSize: Int = 4096) extends StorageInfo {
  var gzipped: Boolean = false

  val recordBuf: ByteBuffer = ByteBuffer.allocate(bufferSize)
  recordBuf.limit(0)
  val headerBuf: ByteBuffer = ByteBuffer.allocate(bufferSize)
  headerBuf.limit(0)
  val hashBuf: ByteBuffer = ByteBuffer.allocate(bufferSize)
  hashBuf.limit(0)

  override def recordStoragePath: Path = sys.error("Inapplicable")
  override def recordReadStream: ReadStream = recordReadWrite
  override def headerReadStream: ReadStream = headerReadWrite
  override def hashReadStream: ReadStream = hashReadWrite

  override def recordReadWrite: ReadWrite = new ReadWriteBuffer(recordBuf)
  override def headerReadWrite: ReadWrite = new ReadWriteBuffer(headerBuf)
  override def hashReadWrite: ReadWrite = new ReadWriteBuffer(hashBuf)

  override def toString: String = "FakeStorageInfo[idx:" + idx + ", buf:" + bufferSize + "]"
  override def name: String = idx.toString
}

// ------------------------------- Real directory -------------------------------

class RealDirectory(basePath: Path) extends Directory {
  import scala.collection.JavaConverters._

  Files.createDirectories(basePath)

  private val recordStoragePaths: mutable.Buffer[Path] = {
    val dirStream = Files.newDirectoryStream(basePath)
    try dirStream.iterator().asScala.filter {path =>
      val s: String = path.toString
      s.endsWith(".record") || s.endsWith(".record.gz")
    }.toBuffer[Path]
    finally dirStream.close()
  }

  private var lastStorageIndex: Int = {
    if (recordStoragePaths.isEmpty) 0
    else {
      recordStoragePaths.view.map {path =>
        val name = path.getFileName.toString
        Try(name.substring(0, name.indexOf('.')).toInt).getOrElse(0)
      }.max
    }
  }

  private val _infos: mutable.Buffer[StorageInfo] = recordStoragePaths.map {path =>
    path.getFileName.toString match {
      case s if s.endsWith(".record") => new RealStorageInfoRW(path)
      case s if s.endsWith(".record.gz") => new RealStorageInfoGzip(path)
    }
  }.toBuffer

  override def infos: Seq[StorageInfo] = _infos
  override def addNewInfo(): StorageInfo = {
    lastStorageIndex += 1
    val newSI = new RealStorageInfoRW(basePath.resolve(Str.zPad(lastStorageIndex, 3) + ".record"))
    _infos += newSI
    newSI
  }

  override def gzipInfo(info: StorageInfo): StorageInfo = {
    if (info.gzipped) return info
    val idx: Int = infos.indexOf(info)
    require(idx >= 0, "No info in this directory")
    val gzPath: Path = FileUtils.gzipFile(info.recordStoragePath)
    val newInfo = new RealStorageInfoGzip(gzPath)
    _infos(idx) = newInfo
    Files.delete(info.recordStoragePath)
    newInfo
  }

  private val lockPath = basePath.resolve("db.lock")
  private var thisLock = false

  override def locked: Boolean = thisLock
  override def canLock: Boolean = !Files.exists(lockPath)
  override def lock(log: Logger, wait: (Int, TimeUnit)): Unit = RealDirectory.lockObj.synchronized {
    require(!locked, "Cannot lock already self-locked storage " + this)
    var waitMillis = wait._2.toMillis(wait._1)
    var firstMessageShown = false
    while (!canLock) {
      if (waitMillis == 0) {
        if (firstMessageShown) sys.error("Cannot acquire lock for " + this + " in " + wait._1 + " " + wait._2.toString)
        else sys.error("Cannot acquire lock for " + this)
      }
      if (!firstMessageShown) {
        log.info("Waiting " + wait._1 + " " + wait._2.toString + " to unlock " + this)
        firstMessageShown = true
      }
      val toSleep = math.max(500, waitMillis)
      Thread.sleep(toSleep)
      waitMillis -= toSleep
    }
    Files.createFile(lockPath)
    thisLock = true
  }
  override def unlock(): Unit = RealDirectory.lockObj.synchronized {
    if (thisLock) {
      Files.deleteIfExists(lockPath)
      thisLock = false
    }
  }

  override def toString: String = "RealDirectory[" + basePath + "]"
}

object RealDirectory {
  private[storage] val lockObj = new Object
}

class RealStorageInfoRW(gotRecordSP: Path, bufferSize: Int = 4096) extends StorageInfo {
  private val baseSP: Path = FileUtils.maybeChopEnding(gotRecordSP, ".record").getOrElse(sys.error("Invalid gotRecordSP: " + gotRecordSP))
  private def makeSP(ending: String): Path = baseSP.resolveSibling(baseSP.getFileName.toString + ending)

  private val recordSP: Path = makeSP(".record")
  private val headerSP: Path = makeSP(".header")
  private val hashSP: Path = makeSP(".hash")

  override def gzipped: Boolean = false
  // будет true только для сжатых gzip'ом файлов хранилищ
  override def recordStoragePath: Path = recordSP

  override def recordReadStream: ReadStream = ReadDataStream.fromPath(recordSP, bufferSize)
  override def headerReadStream: ReadStream = ReadDataStream.fromPath(headerSP, bufferSize)
  override def hashReadStream: ReadStream = ReadDataStream.fromPath(hashSP, bufferSize)

  override def recordReadWrite: ReadWrite = new ReadWriteChannel(recordSP)
  override def headerReadWrite: ReadWrite = new ReadWriteChannel(headerSP)
  override def hashReadWrite: ReadWrite = new ReadWriteChannel(hashSP)

  override def toString: String = "RealStorageInfoRW[" + gotRecordSP + "]"
  override def name: String = gotRecordSP.getFileName.toString
}

class RealStorageInfoGzip(gotRecordSP: Path, bufferSize: Int = 4096) extends StorageInfo {
  private val baseSP: Path = FileUtils.maybeChopEnding(gotRecordSP, ".record.gz").getOrElse(sys.error("Invalid gotRecordSP: " + gotRecordSP))
  private def makeSP(ending: String): Path = baseSP.resolveSibling(baseSP.getFileName.toString + ending)

  private val recordGzipSP: Path = makeSP(".record.gz")
  private val headerSP: Path = makeSP(".header")
  private val hashSP: Path = makeSP(".hash")

  override def gzipped: Boolean = true
  override def recordStoragePath: Path = recordGzipSP

  override def recordReadStream: ReadStream = ReadDataStream.fromPath(recordGzipSP, bufferSize)
  override def headerReadStream: ReadStream = ReadDataStream.fromPath(headerSP, bufferSize)
  override def hashReadStream: ReadStream = ReadDataStream.fromPath(hashSP, bufferSize)

  override def recordReadWrite: ReadWrite = sys.error("Cannot make ReadWrite for gzipped file:" + recordGzipSP)
  override def headerReadWrite: ReadWrite = new ReadWriteChannel(headerSP)
  override def hashReadWrite: ReadWrite = new ReadWriteChannel(hashSP)

  override def toString: String = "RealStorageInfoGzip[" + gotRecordSP + "]"
  override def name: String = gotRecordSP.getFileName.toString
}
