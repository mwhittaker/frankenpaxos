package frankenpaxos.monitoring

import io.prometheus

object PrometheusGaugeCollector
    extends Collector[PrometheusGaugeBuilder, Gauge] {
  def build(): Builder[PrometheusGaugeBuilder, Gauge] =
    new PrometheusGaugeBuilder(prometheus.client.Gauge.build())
}
