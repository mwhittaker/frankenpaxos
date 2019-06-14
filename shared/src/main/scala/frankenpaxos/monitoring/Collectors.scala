package frankenpaxos.monitoring

// The Collectors trait packages up the various collectors to make passing them
// around easier. For example, FakeCollectors includes all the faked Collectors
// and PrometheusCollectors includes all the real Promethus collectors.
trait Collectors {
  type CounterBuilder <: frankenpaxos.monitoring.CounterBuilder[CounterBuilder]
  type GaugeBuilder <: frankenpaxos.monitoring.GaugeBuilder[GaugeBuilder]
  type SummaryBuilder <: frankenpaxos.monitoring.SummaryBuilder[SummaryBuilder]

  def counter: Collector[CounterBuilder, Counter]
  def gauge: Collector[GaugeBuilder, Gauge]
  def summary: Collector[SummaryBuilder, Summary]
}
