package frankenpaxos.monitoring

import io.prometheus

object PrometheusCounterCollector
    extends Collector[PrometheusCounterBuilder, Counter] {
  def build(): Builder[PrometheusCounterBuilder, Counter] =
    new PrometheusCounterBuilder(prometheus.client.Counter.build())
}
