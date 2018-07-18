package zeno

class PrintLogger extends Logger {
  override def fatal(message: String): Unit = {
    def show(s: String): Unit = {
      println(s"${Console.WHITE}${Console.RED_B}[FATAL]${Console.RESET} $s");
    }

    show(message)
    val stackTraceElements =
      for (e <- Thread.currentThread().getStackTrace())
        yield e.toString()
    show(stackTraceElements.mkString("\n"))
    System.exit(1);
  }

  override def error(message: String): Unit = {
    println(s"${Console.RED}[ERROR]${Console.RESET} $message");
  }

  override def warn(message: String): Unit = {
    println(s"${Console.YELLOW}[WARN ]${Console.RESET} $message");
  }

  override def info(message: String): Unit = {
    println(s"${Console.BLUE}[INFO ]${Console.RESET} $message");
  }

  override def debug(message: String): Unit = {
    println(s"${Console.CYAN}[DEBUG]${Console.RESET} $message");
  }
}
