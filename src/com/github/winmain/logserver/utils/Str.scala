package com.github.winmain.logserver.utils
import java.lang.StringBuilder

object Str {
  /**
   * Печатает число строкой с нулями вначале
   *
   * @param value Само число
   * @param length Итоговая длина строки
   */
  def zPad(value: Int, length: Int): String = {
    require(value >= 0)
    val s = value.toString
    if (s.length >= length) s
    else {
      val sb: StringBuilder = new StringBuilder(length)
      var i = length - s.length
      while (i > 0) {sb.append('0'); i -= 1}
      sb.append(s).toString
    }
  }
}
