package zeno

trait Timer {
  def name(): String
  def start(): Unit
  def stop(): Unit
  def reset(): Unit
}
