package frankenpaxos

class PrintLogger extends Logger {
  private val formatter = java.time.format.DateTimeFormatter
    .ofPattern("MMM dd HH:mm:ss.nnnnnnnnn")
    .withZone(java.time.ZoneId.systemDefault())

  private def colored(color: String, s: String) = {
    s"${color}${s}${Console.RESET}"
  }

  private def time: String =
    s"[${formatter.format(java.time.Instant.now())}]"

  private def threadId: String =
    s"[Thread ${Thread.currentThread().getId()}]"

  override def fatal(message: String): Nothing = {
    def show(s: String): Unit = {
      println(
        colored(Console.WHITE + Console.RED_B, s"$time [FATAL] $threadId") + s
      )
    }

    show(message)
    val stackTraceElements =
      for (e <- Thread.currentThread().getStackTrace())
        yield e.toString()
    show(stackTraceElements.mkString("\n"))
    System.exit(1)
    ???
  }

  override def error(message: String): Unit =
    println(colored(Console.RED, s"$time [ERROR] $threadId ") + message)

  override def warn(message: String): Unit =
    println(colored(Console.YELLOW, s"$time [WARN] $threadId ") + message)

  override def info(message: String): Unit =
    println(colored(Console.BLUE, s"$time [INFO] $threadId ") + message)

  override def debug(message: String): Unit =
    println(colored(Console.CYAN, s"$time [DEBUG] $threadId ") + message)
}
