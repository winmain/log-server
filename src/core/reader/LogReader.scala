package core.reader
import java.util.concurrent.BlockingQueue

import core.SourceLogRecord

trait LogReader {
  def readLogs(result: BlockingQueue[SourceLogRecord])
}
