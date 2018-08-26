package zeno

import scala.scalajs.js.annotation._;

@JSExportAll
trait Timer {
  def name(): String
  def start(): Unit
  def stop(): Unit
  def reset(): Unit = {
    stop();
    start();
  }
}
