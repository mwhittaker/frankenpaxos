package zeno

class ScalaLoggingLogger(name: String) extends Logger {
  private val logger = com.typesafe.scalalogging.Logger(name)

  override def error(message: String): Unit = {
    logger.error(message);
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

  override def trace(message: String): Unit = {
    logger.trace(message);
  }
}
