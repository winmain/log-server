import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import command._
import org.slf4j.{Logger, LoggerFactory}

object Cmd extends CommandUtils {
  def commands: Map[String, () => Command] = Map(
    "archive" -> ArchiveCommand,
    "convert-month" -> ConvertMonthCommand,
    "convert-year" -> ConvertYearCommand,
    "get" -> GetCommand.apply,
    "info" -> InfoCommand,
    "update" -> UpdateCommand
  )

  def run(args: Array[String], isDev: Boolean): Unit = {
    if (args.length < 1) {
      println("Usage: <command> [parameters...]")
      println("Available commands: " + commands.keys.toVector.sorted.mkString(", "))
      sys.exit(-1)
    }

    val cmdName = args(0)
    val cmd: Command = commands.getOrElse(cmdName, exitError("Unknown command: " + cmdName))()

    configureLogback(
      if (isDev) "logback-dev.xml"
      else if (cmd.isVerbose) "logback-prod-verbose.xml" else "logback-prod.xml")

    val params: Array[String] = args.drop(1)
    val log: Logger = LoggerFactory.getLogger("main")
    log.info("Run: " + args.mkString(" "))
    cmd.run(log, params)
  }

  private def configureLogback(configFilename: String): Unit = {
    val configurator: JoranConfigurator = new JoranConfigurator
    val ctx: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    configurator.setContext(ctx)
    ctx.reset()
    configurator.doConfigure(getClass.getClassLoader.getResource(configFilename))
  }

  def main(args: Array[String]) {
    run(args, isDev = false)
  }
}
