package zeno

class PrintLogger extends Logger {
  private def colored(color: String, s: String) = {
    s"${color}${s}${Console.RESET}"
  }

  private def withThreadId(s: String): String = {
    s"[Thread ${Thread.currentThread().getId()}] " + s
  }

  override def fatal(message: String): Unit = {
    def show(s: String): Unit = {
      println(
        colored(Console.WHITE + Console.RED_B, withThreadId("[FATAL] ")) + s
      )
    }

    show(message)
    val stackTraceElements =
      for (e <- Thread.currentThread().getStackTrace())
        yield e.toString()
    show(stackTraceElements.mkString("\n"))
    System.exit(1);
  }

  override def error(message: String): Unit = {
    println(colored(Console.RED, withThreadId("[ERROR] ")) + message)
  }

  override def warn(message: String): Unit = {
    println(colored(Console.YELLOW, withThreadId("[WARN] ")) + message)
  }

  override def info(message: String): Unit = {
    println(colored(Console.BLUE, withThreadId("[INFO] ")) + message)
  }

  override def debug(message: String): Unit = {
    println(colored(Console.CYAN, withThreadId("[DEBUG] ")) + message)
  }
}
