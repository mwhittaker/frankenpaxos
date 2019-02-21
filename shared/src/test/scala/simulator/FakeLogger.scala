package frankenpaxos.simulator

import frankenpaxos.Logger
import scala.collection.mutable

class FakeLogger extends Logger {
  val logs = mutable.Buffer[String]()

  override def fatal(message: String): Unit = {
    logs += s"[FATAL] $message"
  }

  override def error(message: String): Unit = {
    logs += s"[ERROR] $message"
  }

  override def warn(message: String): Unit = {
    logs += s"[WARN] $message"
  }

  override def info(message: String): Unit = {
    logs += s"[INFO] $message"
  }

  override def debug(message: String): Unit = {
    logs += s"[DEBUG] $message"
  }
}
