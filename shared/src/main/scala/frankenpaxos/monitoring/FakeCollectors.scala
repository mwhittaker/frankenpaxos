package frankenpaxos.monitoring

object FakeCollectors extends Collectors {
  override type CounterBuilder = FakeCounterBuilder
  override type GaugeBuilder = FakeGaugeBuilder
  override type SummaryBuilder = FakeSummaryBuilder

  override val counter = FakeCounterCollector
  override val gauge = FakeGaugeCollector
  override val summary = FakeSummaryCollector
}
