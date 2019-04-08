package frankenpaxos.monitoring

trait CounterImpl {
  def get(): Double
  def inc(): Unit
  def inc(amt: Double): Unit
}

trait Counter extends CounterImpl {
  type Impl <: CounterImpl
  def labels(labelValues: String*): Impl
}
