package frankenpaxos.monitoring

import io.prometheus

class PrometheusGaugeImpl(child: prometheus.client.Gauge.Child)
    extends GaugeImpl {
  def get(): Double = child.get()
  def set(x: Double): Unit = child.set(x)
  def inc(): Unit = child.inc()
  def inc(amt: Double): Unit = child.inc(amt)
  def dec(): Unit = child.dec()
  def dec(amt: Double): Unit = child.dec(amt)
}

class PrometheusGauge(gauge: prometheus.client.Gauge) extends Gauge {
  type Impl = PrometheusGaugeImpl
  def labels(labelValues: String*): Impl =
    new PrometheusGaugeImpl(gauge.labels(labelValues: _*))
  def get(): Double = gauge.get()
  def set(x: Double): Unit = gauge.set(x)
  def inc(): Unit = gauge.inc()
  def inc(amt: Double): Unit = gauge.inc(amt)
  def dec(): Unit = gauge.dec()
  def dec(amt: Double): Unit = gauge.dec(amt)
}
