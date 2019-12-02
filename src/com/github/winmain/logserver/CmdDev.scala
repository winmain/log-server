package com.github.winmain.logserver

import com.github.winmain.logserver.db.storage.BigStorageLocks

object CmdDev {
  def main(args: Array[String]) {
    try {
      Cmd.run(args, isDev = true)
    } finally {
      BigStorageLocks.unlockAll()
    }
  }
}
