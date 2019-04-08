package frankenpaxos.monitoring

object FakeCollectors extends Collectors {
  type CounterBuilder = FakeCounterBuilder
  type GaugeBuilder = FakeGaugeBuilder

  val counter = FakeCounterCollector
  val gauge = FakeGaugeCollector
}
