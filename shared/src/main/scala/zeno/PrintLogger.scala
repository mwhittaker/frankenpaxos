package zeno

class PrintLogger extends Logger {
  override def error(message: String): Unit = {
    println("[ERROR] " + message);
  }

  override def warn(message: String): Unit = {
    println("[WARN ] " + message);
  }

  override def info(message: String): Unit = {
    println("[INFO ] " + message);
  }

  override def debug(message: String): Unit = {
    println("[DEBUG] " + message);
  }

  override def trace(message: String): Unit = {
    println("[TRACE] " + message);
  }
}
