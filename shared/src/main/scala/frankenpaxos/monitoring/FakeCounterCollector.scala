package frankenpaxos.monitoring

object FakeCounterCollector extends Collector[FakeCounterBuilder, Counter] {
  def build(): Builder[FakeCounterBuilder, Counter] = new FakeCounterBuilder()
}
