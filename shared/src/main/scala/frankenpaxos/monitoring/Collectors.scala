package frankenpaxos.monitoring

trait Collectors {
  type CounterBuilder <: Builder[CounterBuilder, Counter]

  def counter: Collector[CounterBuilder, Counter]
}
