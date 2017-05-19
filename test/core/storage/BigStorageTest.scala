package core.storage
import core.storage.Storage.Record
import org.slf4j.Logger
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class BigStorageTest extends Specification with Mockito {
  val rec1 = Record(55L, "user", 1, "some data".getBytes)
  val rec2 = Record(61L, "user", 1, "another data".getBytes)
  val rec3 = Record(43L, "user", 2, "second user".getBytes)
  val rec4 = Record(80L, "account", 5, "some account".getBytes)
  val rec5 = Record(85L, "account", 5, "another 5 account".getBytes)

  "simple appendable write & read" in {
    val dir = new FakeDirectory()
    // step1: write appendable
    val log = mock[Logger]
    val abs: AppendableBigStorage = new AppendableBigStorage(dir, log = log)
    abs.addRecord(rec1) === true
    abs.addRecord(rec2) === true
    abs.addRecord(rec3) === true
    abs.addRecord(rec4) === true
    abs.addRecord(rec1) === false
    abs.close()
    no(log).warn(anyString)
    no(log).warn(anyString, any[Throwable])

    dir.infos.length === 1
    val info: FakeStorageInfo = dir.infos.head
    info.recordBuf.limit() must be > 10
    info.headerBuf.limit() must be > 10
    info.hashBuf.limit() must be > 10

    // step2: read read-only
    val rbs: ReadOnlyBigStorage = new ReadOnlyBigStorage(dir)
    rbs.getRecords("user", 1) === Vector(rec1, rec2)
    rbs.close()
    success
  }


  "write multiple storages" in {
    val dir = new FakeDirectory()
    // step1: write appendable
    val opts = new StorageOpts
    val log = mock[Logger]
    opts.maxRecordNum = 2
    locally {
      val abs: AppendableBigStorage = new AppendableBigStorage(dir, opts, log)
      abs.addRecord(rec1) === true
      abs.addRecord(rec2) === true
      dir.infos.length === 1
      abs.addRecord(rec3) === true
      dir.infos.length === 2
      abs.addRecord(rec4) === true
      abs.addRecord(rec1) === false
      dir.infos.length === 2
      abs.close()
      no(log).warn(anyString)
      no(log).warn(anyString, any[Throwable])

      var info: FakeStorageInfo = dir.infos(0)
      info.recordBuf.limit() must be > 10
      info.headerBuf.limit() must be > 10
      info.hashBuf.limit() must be > 10

      info = dir.infos(1)
      info.recordBuf.limit() must be > 10
      info.headerBuf.limit() must be > 10
      info.hashBuf.limit() must be > 10
    }

    // step2: open & append to BigStorage
    locally {
      val abs: AppendableBigStorage = new AppendableBigStorage(dir, opts, log)
      dir.infos.length === 2
      abs.addRecord(rec2) === false
      abs.addRecord(rec4) === false
      abs.addRecord(rec5) === true
      dir.infos.length === 3
      abs.close()
      no(log).warn(anyString)
      no(log).warn(anyString, any[Throwable])
    }

    // step3: open & check BigStorage
    locally {
      val rbs: ReadOnlyBigStorage = new ReadOnlyBigStorage(dir, opts, log)
      rbs.getRecords("user", 1) === Vector(rec1, rec2)
      rbs.getRecords("account", 5) === Vector(rec4, rec5)
      rbs.getRecords("account", 1) === Nil
      rbs.close()
      no(log).warn(anyString)
      no(log).warn(anyString, any[Throwable])
    }
    success
  }


  class DeleteContext(optFn: StorageOpts => Any = _ => ()) extends Scope {
    val dir = new FakeDirectory()
    val log = mock[Logger]
    val opts = new StorageOpts

    // step1: write appendable
    locally {
      optFn(opts)
      val abs: AppendableBigStorage = new AppendableBigStorage(dir, log = log, opts = opts)
      abs.addRecord(rec1) === true
      abs.addRecord(rec2) === true
      abs.close()
      no(log).warn(anyString)
      no(log).warn(anyString, any[Throwable])
    }

    def checkReadOnlyBigStorage(): Unit = {
      val rbs = new ReadOnlyBigStorage(dir, log = log)
      rbs.getRecords("user", 1) === Vector(rec1, rec2)
      rbs.close()
    }
  }

  "ReadOnlyBigStorage delete headers" in new DeleteContext {
    dir.infos(0).headerBuf.limit(0)
    checkReadOnlyBigStorage()
    one(log).warn(argThat((_: String).contains("Recovering headers")))
  }

  "ReadOnlyBigStorage delete hashes" in new DeleteContext {
    dir.infos(0).hashBuf.limit(0)
    checkReadOnlyBigStorage()
    no(log).warn(anyString)
    no(log).warn(anyString, any[Throwable])
  }

  "AppendableBigStorage delete headers" in new DeleteContext {
    dir.infos(0).headerBuf.limit(0)
    val abs = new AppendableBigStorage(dir, log = log)
    abs.addRecord(rec1) === false
    abs.addRecord(rec3) === true
    abs.close()
    one(log).warn(argThat((_: String).contains("Recovering headers")))
  }

  "AppendableBigStorage delete hashes in one storage" in new DeleteContext {
    dir.infos(0).hashBuf.limit(0)
    val abs = new AppendableBigStorage(dir, log = log)
    abs.addRecord(rec1) === false
    abs.addRecord(rec3) === true
    abs.close()
    one(log).warn(argThat((_: String).contains("Recovered headers")))
  }

  "AppendableBigStorage delete hashes in two storages" in new DeleteContext(_.maxRecordNum = 2) {
    dir.infos(0).hashBuf.limit(0)
    val abs = new AppendableBigStorage(dir, log = log)
    abs.addRecord(rec1) === false
    abs.addRecord(rec3) === true
    abs.close()
    one(log).warn(argThat((_: String).contains("Recovered headers")))
  }

  "AppendableBigStorage delete headers and reopen twice" in new DeleteContext {
    locally {
      dir.infos(0).headerBuf.limit(0)
      dir.infos(0).hashBuf.limit(0)
      val abs = new AppendableBigStorage(dir, log = log)
      abs.addRecord(rec1) === false
      abs.close()
      one(log).warn(argThat((_: String).contains("Recovering headers")))
    }
    locally {
      val log = mock[Logger]
      val abs = new AppendableBigStorage(dir, log = log)
      abs.addRecord(rec1) === false
      abs.close()
      no(log).warn(anyString)
    }
    success
  }


  "crushed storage and old headers and hashes" in {
    val dir = new FakeDirectory()
    val log = mock[Logger]
    val opts = new StorageOpts

    // step1: write first records as usual
    locally {
      val abs: AppendableBigStorage = new AppendableBigStorage(dir, log = log, opts = opts)
      abs.addRecord(rec1) === true
      abs.addRecord(rec2) === true
      abs.close()
    }
    // step2: write one record, but not headers
    locally {
      val abs: AppendableBigStorage = new AppendableBigStorage(dir, log = log, opts = opts)
      abs.addRecord(rec3) === true
      abs.storages(0).asInstanceOf[abs.AppendableStorage].ars.close()
      abs.unlock()
    }
    no(log).warn(anyString)
    no(log).warn(anyString, any[Throwable])
    // step3: try to re-add record
    // Здесь мы получаем recordStorage с 3 запиями, и header/hash storage с 2 записями.
    locally {
      val abs: AppendableBigStorage = new AppendableBigStorage(dir, log = log, opts = opts)
      abs.addRecord(rec3) === false
      abs.addRecord(rec4) === true
      one(log).warn(argThat((_: String).contains("Recovering headers")))
    }
    success
  }
}
