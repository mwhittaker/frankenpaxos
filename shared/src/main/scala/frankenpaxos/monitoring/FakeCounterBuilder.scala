package frankenpaxos.monitoring

// TODO(mwhittaker): Incorporate builder information into FakeCounter.
class FakeCounterBuilder extends Builder[FakeCounterBuilder, Counter] {
  def help(help: String): FakeCounterBuilder = this
  def labelNames(labelNames: String*): FakeCounterBuilder = this
  def name(name: String): FakeCounterBuilder = this
  def namespace(namespace: String): FakeCounterBuilder = this
  def register(): Counter = new FakeCounter()
}
