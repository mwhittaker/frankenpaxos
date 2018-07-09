package zeno

class PrintLogger extends Logger {
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

  override def trace(message: String): Unit = {
    println(s"[TRACE] " + message);
  }
}
