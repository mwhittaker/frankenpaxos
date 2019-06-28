package frankenpaxos

import Ordering.Implicits._

sealed trait LogLevel
object LogDebug extends LogLevel
object LogInfo extends LogLevel
object LogWarn extends LogLevel
object LogError extends LogLevel
object LogFatal extends LogLevel

object LogLevel {
  implicit val read: scopt.Read[LogLevel] = scopt.Read.reads({
    case "debug" => LogDebug
    case "info"  => LogInfo
    case "warn"  => LogWarn
    case "error" => LogError
    case "fatal" => LogFatal
    case x =>
      throw new IllegalArgumentException(
        s"$x is not one of debug, info, warn, error, or fatal."
      )
  })
}

abstract class Logger(logLevel: LogLevel) {
  // Logging.
  def fatal(message: String): Nothing = {
    fatalImpl(message)
  }

  def error(message: String): Unit = {
    logLevel match {
      case LogDebug | LogInfo | LogWarn | LogError => errorImpl(message)
      case LogFatal                                =>
    }
  }

  def warn(message: String): Unit = {
    logLevel match {
      case LogDebug | LogInfo | LogWarn => warnImpl(message)
      case LogError | LogFatal          =>
    }
  }

  def info(message: String): Unit = {
    logLevel match {
      case LogDebug | LogInfo            => infoImpl(message)
      case LogWarn | LogError | LogFatal =>
    }
  }

  def debug(message: String): Unit = {
    logLevel match {
      case LogDebug                                => debugImpl(message)
      case LogInfo | LogWarn | LogError | LogFatal =>
    }
  }

  // Logging implementations.
  def fatalImpl(message: String): Nothing
  def errorImpl(message: String): Unit
  def warnImpl(message: String): Unit
  def infoImpl(message: String): Unit
  def debugImpl(message: String): Unit

  // Checking.
  def check(b: Boolean): Unit = {
    if (!b) {
      fatal("Check failed!")
    }
  }

  def checkEq[A](lhs: A, rhs: A): Unit = {
    if (lhs != rhs) {
      fatal(s"Check failed: $lhs != $rhs.")
    }
  }

  def checkNe[A](lhs: A, rhs: A): Unit = {
    if (lhs == rhs) {
      fatal(s"Check failed: $lhs == $rhs.")
    }
  }

  def checkLt[A: Ordering](lhs: A, rhs: A): Unit = {
    if (lhs >= rhs) {
      fatal(s"Check failed: $lhs >= $rhs.")
    }
  }

  def checkLe[A: Ordering](lhs: A, rhs: A): Unit = {
    if (lhs > rhs) {
      fatal(s"Check failed: $lhs > $rhs.")
    }
  }

  def checkGt[A: Ordering](lhs: A, rhs: A): Unit = {
    if (lhs <= rhs) {
      fatal(s"Check failed: $lhs <= $rhs.")
    }
  }

  def checkGe[A: Ordering](lhs: A, rhs: A): Unit = {
    if (lhs < rhs) {
      fatal(s"Check failed: $lhs < $rhs.")
    }
  }
}
