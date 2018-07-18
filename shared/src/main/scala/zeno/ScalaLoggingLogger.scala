package zeno

class ScalaLoggingLogger(name: String) extends Logger {
  private val logger = com.typesafe.scalalogging.Logger(name)

  override def fatal(message: String): Unit = {
    logger.error(message);
    val stackTraceElements =
      for (e <- Thread.currentThread().getStackTrace())
        yield e.toString()
    logger.error(stackTraceElements.mkString("\n"))
    System.exit(1);
  }

  override def error(message: String): Unit = {
    logger.error(message);
    val stackTraceElements =
      for (e <- Thread.currentThread().getStackTrace())
        yield e.toString()
    logger.error(stackTraceElements.mkString("\n"))
  }

  override def warn(message: String): Unit = {
    logger.warn(message);
  }

  override def info(message: String): Unit = {
    logger.info(message);
  }

  override def debug(message: String): Unit = {
    logger.debug(message);
  }
}
