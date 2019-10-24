package com.github.winmain.logserver.db

import org.specs2.mutable.Specification

class ArrayStorageSpec extends Specification {
  "LongStorage" should {
    "Empty storage on 0" in {
      ArrayStorage.newLongStorage(0) must beAnInstanceOf[EmptyStorageLong]
    }

    "Int storage" in {
      var storage: ArrayStorage[Long] = ArrayStorage.newLongStorage(0)
      storage.add(0) must beNone
      storage = storage.add(1).get
      storage.add(-100) must beNone
      storage.add(50000) must beNone
      storage must beAnInstanceOf[IntArrayStorageLong]
      storage.size === 4
      storage.apply(0) === 0
      storage.apply(1) === 1
      storage.apply(2) === -100
      storage.apply(3) === 50000
      storage.apply(-1) must throwA[IllegalArgumentException]
      storage.apply(4) must throwA[IllegalArgumentException]
    }

    "Empty storage overflow" in {
      val storage: ArrayStorage[Long] = ArrayStorage.newLongStorage(125)
      for (i <- 0 until ArrayStorage.MaxSize) storage.add(10)
      storage.add(500) must throwA("too many elements")
    }

    "Long storage transition" in {
      var storage: ArrayStorage[Long] = ArrayStorage.newLongStorage(0)
      storage = storage.add(1).get
      storage.add(Int.MaxValue) must beNone
      storage.add(Int.MinValue) must beNone
      storage = storage.add(Int.MaxValue + 1L).get
      storage.add(Long.MaxValue) must beNone
      storage.add(Long.MinValue) must beNone
      storage must beAnInstanceOf[LongArrayStorage]
      storage.size === 6
      storage.apply(0) === 1
      storage.apply(1) === Int.MaxValue
      storage.apply(2) === Int.MinValue
      storage.apply(3) === (Int.MaxValue + 1L)
      storage.apply(4) === Long.MaxValue
      storage.apply(5) === Long.MinValue
    }
  }

  "StringStorage" should {
    "Empty storage on \"\"" in {
      ArrayStorage.newStringStorage("") must beAnInstanceOf[EmptyStorageString]
    }

    "Int storage" in {
      var storage: ArrayStorage[String] = ArrayStorage.newStringStorage("")
      storage.add("") must beNone
      storage = storage.add("0").get
      storage.add("-100") must beNone
      storage.add("50000") must beNone
      storage.add(Int.MaxValue.toString) must beNone
      storage.add((Int.MinValue + 1).toString) must beNone
      storage.add(Int.MinValue.toString) must beSome(anInstanceOf[LongArrayStorageString])
      storage must beAnInstanceOf[IntArrayStorageString]
      storage.size === 6
      storage.apply(0) === ""
      storage.apply(1) === "0"
      storage.apply(2) === "-100"
      storage.apply(3) === "50000"
      storage.apply(4) === Int.MaxValue.toString
      storage.apply(5) === (Int.MinValue + 1).toString
    }

    "Int-Long-String storage transition" in {
      var storage: ArrayStorage[String] = ArrayStorage.newStringStorage("")
      storage.add("") must beNone
      storage = storage.add("5").get
      storage must beAnInstanceOf[IntArrayStorageString]
      storage = storage.add(0x500000000L.toString).get
      storage must beAnInstanceOf[LongArrayStorageString]
      storage.add("1234567890123") must beNone
      storage.add("-3210987654321") must beNone
      storage.add("") must beNone
      storage.add(Long.MaxValue.toString) must beNone
      storage.add((Long.MinValue + 1).toString) must beNone
      storage.add(Long.MinValue.toString) must beSome(anInstanceOf[StringArrayStorage])
      storage.size === 8
      storage.apply(0) === ""
      storage.apply(1) === "5"
      storage.apply(2) === 0x500000000L.toString
      storage.apply(3) === "1234567890123"
      storage.apply(4) === "-3210987654321"
      storage.apply(5) === ""
      storage.apply(6) === Long.MaxValue.toString
      storage.apply(7) === (Long.MinValue + 1).toString
    }

    "Test numbers in strings equality" in {
      def testStorage(storage: ArrayStorage[String]) = {
        storage.add(" ") must beSome(anInstanceOf[StringArrayStorage])
        storage.add(" 1") must beSome(anInstanceOf[StringArrayStorage])
        storage.add("1 ") must beSome(anInstanceOf[StringArrayStorage])
        storage.add("025") must beSome(anInstanceOf[StringArrayStorage])
        storage.add("-07") must beSome(anInstanceOf[StringArrayStorage])
        storage.add("5.0") must beSome(anInstanceOf[StringArrayStorage])
        storage.add("+83") must beSome(anInstanceOf[StringArrayStorage])
      }
      locally {
        val storage: ArrayStorage[String] = ArrayStorage.newStringStorage("1")
        storage must beAnInstanceOf[IntArrayStorageString]
        testStorage(storage)
      }
      locally {
        val storage: ArrayStorage[String] = ArrayStorage.newStringStorage("1234567890123")
        storage must beAnInstanceOf[LongArrayStorageString]
        testStorage(storage)
      }
    }
  }
}
