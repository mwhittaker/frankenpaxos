package frankenpaxos.monitoring

trait Collectors {
  type CounterBuilder <: Builder[CounterBuilder, Counter]
  type GaugeBuilder <: Builder[GaugeBuilder, Gauge]

  def counter: Collector[CounterBuilder, Counter]
  def gauge: Collector[GaugeBuilder, Gauge]
}
