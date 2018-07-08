package zeno

class PrintLogger extends Logger {
  override def error(message: String): Unit = {
    println(s"${Console.RED}[ERROR] $message${Console.RESET}");
  }

  override def warn(message: String): Unit = {
    println(s"${Console.YELLOW}[WARN ] $message${Console.RESET}");
  }

  override def info(message: String): Unit = {
    println(s"[INFO ] " + message);
  }

  override def debug(message: String): Unit = {
    println(s"${Console.BLUE}[DEBUG] $message${Console.RESET}");
  }

  override def trace(message: String): Unit = {
    println(s"[TRACE] " + message);
  }
}
