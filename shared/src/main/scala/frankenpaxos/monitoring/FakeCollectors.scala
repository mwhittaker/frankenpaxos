package frankenpaxos.monitoring

object FakeCollectors extends Collectors {
  type CounterBuilder = FakeCounterBuilder

  val counter = FakeCounterCollector
}
