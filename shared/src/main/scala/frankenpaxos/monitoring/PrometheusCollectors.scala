package frankenpaxos.monitoring

object PrometheusCollectors extends Collectors {
  type CounterBuilder = PrometheusCounterBuilder

  val counter = PrometheusCounterCollector
}
