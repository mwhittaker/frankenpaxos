package frankenpaxos.monitoring

import io.prometheus

object PrometheusGaugeCollector
    extends Collector[PrometheusGaugeBuilder, Gauge] {
  override def build(): Builder[PrometheusGaugeBuilder, Gauge] =
    new PrometheusGaugeBuilder(prometheus.client.Gauge.build())
}

class PrometheusGaugeBuilder(builder: prometheus.client.Gauge.Builder)
    extends GaugeBuilder[PrometheusGaugeBuilder] {
  override def help(help: String): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.help(help))

  override def labelNames(labelNames: String*): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.labelNames(labelNames: _*))

  override def name(name: String): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.name(name))

  override def namespace(namespace: String): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.namespace(namespace))

  override def register(): Gauge =
    new PrometheusGauge(builder.register())
}

class PrometheusGaugeImpl(child: prometheus.client.Gauge.Child)
    extends GaugeImpl {
  override def get(): Double = child.get()
  override def set(x: Double): Unit = child.set(x)
  override def inc(): Unit = child.inc()
  override def inc(amt: Double): Unit = child.inc(amt)
  override def dec(): Unit = child.dec()
  override def dec(amt: Double): Unit = child.dec(amt)
}

class PrometheusGauge(gauge: prometheus.client.Gauge) extends Gauge {
  override type Impl = PrometheusGaugeImpl
  override def labels(labelValues: String*): Impl =
    new PrometheusGaugeImpl(gauge.labels(labelValues: _*))
  override def get(): Double = gauge.get()
  override def set(x: Double): Unit = gauge.set(x)
  override def inc(): Unit = gauge.inc()
  override def inc(amt: Double): Unit = gauge.inc(amt)
  override def dec(): Unit = gauge.dec()
  override def dec(amt: Double): Unit = gauge.dec(amt)
}
