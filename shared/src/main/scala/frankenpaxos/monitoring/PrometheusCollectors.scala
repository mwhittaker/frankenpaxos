package frankenpaxos.monitoring

object PrometheusCollectors extends Collectors {
  override type CounterBuilder = PrometheusCounterBuilder
  override type GaugeBuilder = PrometheusGaugeBuilder
  override type SummaryBuilder = PrometheusSummaryBuilder

  override val counter = PrometheusCounterCollector
  override val gauge = PrometheusGaugeCollector
  override val summary = PrometheusSummaryCollector
}
