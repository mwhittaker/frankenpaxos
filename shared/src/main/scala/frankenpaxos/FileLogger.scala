package frankenpaxos

class FileLogger(filename: String) extends Logger {
  private val file = new java.io.File(filename)
  private val writer = new java.io.PrintWriter(file)
  private val formatter = java.time.format.DateTimeFormatter
    .ofPattern("MMM dd HH:mm:ss.nnnnnnnnn")
    .withZone(java.time.ZoneId.systemDefault())

  private def time: String =
    s"[${formatter.format(java.time.Instant.now())}]"

  private def threadId: String =
    s"[Thread ${Thread.currentThread().getId()}]"

  override def fatal(message: String): Nothing = {
    writer.println(s"$time [FATAL] $threadId " + message)
    val stackTraceElements =
      for (e <- Thread.currentThread().getStackTrace())
        yield e.toString()
    writer.println(stackTraceElements.mkString("\n"))
    writer.flush()
    System.exit(1)
    ???
  }

  override def error(message: String): Unit = {
    writer.println(s"$time [ERROR] $threadId $message")
    writer.flush()
  }

  override def warn(message: String): Unit = {
    writer.println(s"$time [WARN] $threadId $message")
    writer.flush()
  }

  override def info(message: String): Unit = {
    writer.println(s"$time [INFO] $threadId $message")
    writer.flush()
  }

  override def debug(message: String): Unit = {
    writer.println(s"$time [DEBUG] $threadId $message")
    writer.flush()
  }
}
