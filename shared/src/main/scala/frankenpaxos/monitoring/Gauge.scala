package frankenpaxos.monitoring

// The Gauge trait mimics the Prometheus Java library's Gauge class, and
// GaugeImpl mimics the Gauge.Child class [1].
//
// [1]: http://prometheus.github.io/client_java/io/prometheus/client/Gauge.html
//
trait GaugeBuilder[Self <: GaugeBuilder[Self]] extends Builder[Self, Gauge] {}

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
  def get(): Double
  def set(x: Double): Unit
  def inc(): Unit
  def inc(amt: Double): Unit
  def dec(): Unit
  def dec(amt: Double): Unit
}
