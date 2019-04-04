package frankenpaxos.monitoring

trait Counter {
  def get(): Double
  def inc(): Unit
  def inc(amt: Double): Unit
}
