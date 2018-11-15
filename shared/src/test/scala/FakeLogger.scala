package zeno

import scala.collection.mutable

class FakeLogger extends Logger {
  val logs = mutable.Buffer[Seq[String]]()
  val buffer = mutable.Buffer[String]()

  def flush(): Unit = {
    logs += buffer.to[Seq]
    buffer.clear()
  }

  override def fatal(message: String): Unit = {
    buffer += s"[FATAL] $message"
  }

  override def error(message: String): Unit = {
    buffer += s"[ERROR] $message"
  }

  override def warn(message: String): Unit = {
    buffer += s"[WARN] $message"
  }

  override def info(message: String): Unit = {
    buffer += s"[INFO] $message"
  }

  override def debug(message: String): Unit = {
    buffer += s"[DEBUG] $message"
  }
}
