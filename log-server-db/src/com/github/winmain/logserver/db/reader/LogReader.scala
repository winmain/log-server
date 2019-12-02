package com.github.winmain.logserver.db.reader
import java.util.concurrent.BlockingQueue

import com.github.winmain.logserver.db.SourceLogRecord

trait LogReader {
  def readLogs(result: BlockingQueue[SourceLogRecord])
}
