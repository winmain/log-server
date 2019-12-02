package utils

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.github.winmain.logserver.db.utils.InputStreamSplitter
import org.specs2.mutable.Specification

class InputStreamSplitterSpec extends Specification {
  def charset = StandardCharsets.UTF_8

  def split(source: String, delimiter: String, bufferStep: Int): Seq[String] = {
    val splitter = new InputStreamSplitter(new ByteArrayInputStream(source.getBytes(charset)), delimiter.getBytes(charset), bufferStep)
    val ret = Seq.newBuilder[String]
    while (splitter.readNext()) {
      ret += new String(splitter.getBuf, splitter.getStart, splitter.getLength, charset)
    }
    ret.result()
  }

  "no split" in {
    val splitter = new InputStreamSplitter(new ByteArrayInputStream("abc".getBytes(charset)), " ".getBytes(charset), 2)
    splitter.readNext() === true
    splitter.getStart === 0
    splitter.getEnd === 3
    splitter.readNext() === false
  }
  "on split2" in {
    split("abcde", "ee", 2) === Seq("abcde")
  }
  "one split" in {
    split("abcde", "cd", 2) === Seq("ab", "cde")
  }
  "all string is delimiter" in {
    split("abcde", "abcde", 2) === Seq("", "abcde")
  }
  "split at start" in {
    split("abcde", "ab", 2) === Seq("", "abcde")
  }
  "split at end" in {
    split("abcde", "de", 2) === Seq("abc", "de")
  }
  "two splits" in {
    split("test", "t", 2) === Seq("", "tes", "t")
  }
  "more splits" in {
    split("test string", "t", 1) === Seq("", "tes", "t s", "tring")
    split("test string", "t", 2) === Seq("", "tes", "t s", "tring")
    split("test string", "t", 3) === Seq("", "tes", "t s", "tring")
    split("test string", "t", 4) === Seq("", "tes", "t s", "tring")
    split("test string", "t", 5) === Seq("", "tes", "t s", "tring")
    split("test string", "t", 6) === Seq("", "tes", "t s", "tring")
    split("test string", "t", 7) === Seq("", "tes", "t s", "tring")
    split("test string", "t", 8) === Seq("", "tes", "t s", "tring")
  }
  "string splitted by char" in {
    split("a test string", "t", 8) === Seq("a ", "tes", "t s", "tring")
  }
  "repeatable delimiter" in {
    split("a---b", "--", 1) === Seq("a", "---b")
    split("a---b", "--", 2) === Seq("a", "---b")
    split("a---b", "--", 4) === Seq("a", "---b")
  }
  "repeatable delimiter2" in {
    split("a----b", "--", 2) === Seq("a", "--", "--b")
  }
}
