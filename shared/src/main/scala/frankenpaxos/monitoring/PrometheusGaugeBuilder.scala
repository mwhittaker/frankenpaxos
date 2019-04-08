package frankenpaxos.monitoring

import io.prometheus

class PrometheusGaugeBuilder(builder: prometheus.client.Gauge.Builder)
    extends Builder[PrometheusGaugeBuilder, Gauge] {
  def help(help: String): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.help(help))

  def labelNames(labelNames: String*): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.labelNames(labelNames: _*))

  def name(name: String): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.name(name))

  def namespace(namespace: String): PrometheusGaugeBuilder =
    new PrometheusGaugeBuilder(builder.namespace(namespace))

  def register(): Gauge =
    new PrometheusGauge(builder.register())
}
