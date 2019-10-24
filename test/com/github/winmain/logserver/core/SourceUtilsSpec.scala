package com.github.winmain.logserver.core
import org.specs2.mutable.Specification

class SourceUtilsSpec extends Specification {
  "toStringDetectEncoding" should {
    "read utf-8" in {
      SourceUtils.toStringDetectEncoding("тестовая СТРОКА".getBytes("utf-8")) === "тестовая СТРОКА"
    }
    "read cp1251" in {
      SourceUtils.toStringDetectEncoding("тестовая СТРОКА".getBytes("cp1251")) === "тестовая СТРОКА"
    }
  }
}
