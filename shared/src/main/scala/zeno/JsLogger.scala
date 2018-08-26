package zeno

import scala.collection.mutable.Buffer
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._;

// TODO(mwhittaker): Allow javascript to add custom colors.
@JSExportAll
class JsLogger extends Logger {
  private val bufferedLogs = Buffer[String]()
  def bufferedLogsJs(): js.Array[String] = { bufferedLogs.toJSArray }
  def clearBufferedLogs(): Unit = { bufferedLogs.clear() }

  override def fatal(message: String): Unit = {
    bufferedLogs += s"[FATAL] $message"
  }

  override def error(message: String): Unit = {
    bufferedLogs += s"[ERROR] $message"
  }

  override def warn(message: String): Unit = {
    bufferedLogs += s"[WARN ] $message"
  }

  override def info(message: String): Unit = {
    bufferedLogs += s"[INFO ] $message"
  }

  override def debug(message: String): Unit = {
    bufferedLogs += s"[DEBUG] $message"
  }
}
