package frankenpaxos

class PrintLogger(logLevel: LogLevel = LogDebug) extends Logger(logLevel) {
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

  override def fatalImpl(message: String): Nothing = {
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

  override def errorImpl(message: String): Unit =
    println(colored(Console.RED, s"$time [ERROR] $threadId ") + message)

  override def warnImpl(message: String): Unit =
    println(colored(Console.YELLOW, s"$time [WARN] $threadId ") + message)

  override def infoImpl(message: String): Unit =
    println(colored(Console.BLUE, s"$time [INFO] $threadId ") + message)

  override def debugImpl(message: String): Unit =
    println(colored(Console.CYAN, s"$time [DEBUG] $threadId ") + message)
}
