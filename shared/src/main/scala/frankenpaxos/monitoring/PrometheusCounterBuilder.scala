package frankenpaxos.monitoring

import io.prometheus

class PrometheusCounterBuilder(builder: prometheus.client.Counter.Builder)
    extends Builder[PrometheusCounterBuilder, Counter] {
  def help(help: String): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.help(help))

  def labelNames(labelNames: String*): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.labelNames(labelNames: _*))

  def name(name: String): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.name(name))

  def namespace(namespace: String): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.namespace(namespace))

  def register(): Counter =
    new PrometheusCounter(builder.register())
}
