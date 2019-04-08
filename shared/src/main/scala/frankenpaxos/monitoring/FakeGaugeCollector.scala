package frankenpaxos.monitoring

object FakeGaugeCollector extends Collector[FakeGaugeBuilder, Gauge] {
  def build(): Builder[FakeGaugeBuilder, Gauge] = new FakeGaugeBuilder()
}
