package com.github.winmain.logserver.core.reader
import java.util.concurrent.BlockingQueue

import com.github.winmain.logserver.core.SourceLogRecord

trait LogReader {
  def readLogs(result: BlockingQueue[SourceLogRecord])
}
