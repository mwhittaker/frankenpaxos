package zeno

trait Logger {
  // Logging.
  def fatal(message: String): Unit
  def error(message: String): Unit
  def warn(message: String): Unit
  def info(message: String): Unit
  def debug(message: String): Unit

  // Checking.
  def check(b: Boolean): Unit = {
    if (!b) {
      fatal("Check failed!")
    }
  }

  def check_eq[A](lhs: A, rhs: A): Unit = {
    if (lhs != rhs) {
      fatal(s"Check failed: $lhs != $rhs.")
    }
  }

  def check_ne[A](lhs: A, rhs: A): Unit = {
    if (lhs == rhs) {
      fatal(s"Check failed: $lhs == $rhs.")
    }
  }
}
