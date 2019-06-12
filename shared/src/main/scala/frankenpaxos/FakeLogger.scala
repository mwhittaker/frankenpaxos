package frankenpaxos.simulator

import frankenpaxos.LogDebug
import frankenpaxos.LogLevel
import frankenpaxos.Logger
import scala.collection.mutable

class FakeLogger(logLevel: LogLevel = LogDebug) extends Logger(logLevel) {
  val logs = mutable.Buffer[String]()

  override def fatalImpl(message: String): Nothing = {
    logs += s"[FATAL] $message"
    throw new AssertionError(message)
  }

  override def errorImpl(message: String): Unit = {
    logs += s"[ERROR] $message"
  }

  override def warnImpl(message: String): Unit = {
    logs += s"[WARN] $message"
  }

  override def infoImpl(message: String): Unit = {
    logs += s"[INFO] $message"
  }

  override def debugImpl(message: String): Unit = {
    logs += s"[DEBUG] $message"
  }
}
