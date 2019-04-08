package frankenpaxos.monitoring

object PrometheusCollectors extends Collectors {
  type CounterBuilder = PrometheusCounterBuilder
  type GaugeBuilder = PrometheusGaugeBuilder

  val counter = PrometheusCounterCollector
  val gauge = PrometheusGaugeCollector
}
