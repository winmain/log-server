package com.github.winmain.logserver.core
import java.nio.charset.{Charset, StandardCharsets}

object SourceUtils {
  private val CP1251 = Charset.forName("cp1251")

  def toStringDetectEncoding(bytes: Array[Byte], offset: Int, len: Int): String = {
    val s: String = new String(bytes, offset, len, StandardCharsets.UTF_8)
    if (s.indexOf(0xfffd) >= 0) new String(bytes, offset, len, CP1251)
    else s
  }
  def toStringDetectEncoding(bytes: Array[Byte]): String = toStringDetectEncoding(bytes, 0, bytes.length)
}
