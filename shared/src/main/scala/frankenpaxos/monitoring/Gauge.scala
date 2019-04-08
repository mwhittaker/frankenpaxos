package frankenpaxos.monitoring

trait GaugeImpl {
  def get(): Double
  def set(x: Double): Unit
  def inc(): Unit
  def inc(amt: Double): Unit
  def dec(): Unit
  def dec(amt: Double): Unit
}

trait Gauge extends GaugeImpl {
  type Impl <: GaugeImpl
  def labels(labelValues: String*): Impl
}
