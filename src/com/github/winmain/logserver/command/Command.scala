package com.github.winmain.logserver.command
import org.slf4j.Logger

abstract class CommandUtils {
  protected def exitError(error: String): Nothing = {
    println(error)
    sys.exit(-1)
  }
}


abstract class Command extends CommandUtils {
  def isVerbose: Boolean = true
  def run(log: Logger, params: Array[String]): Unit
}
