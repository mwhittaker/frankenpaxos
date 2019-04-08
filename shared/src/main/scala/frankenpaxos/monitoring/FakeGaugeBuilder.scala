package frankenpaxos.monitoring

// TODO(mwhittaker): Incorporate builder information into FakeGauge.
class FakeGaugeBuilder extends Builder[FakeGaugeBuilder, Gauge] {
  def help(help: String): FakeGaugeBuilder = this
  def labelNames(labelNames: String*): FakeGaugeBuilder = this
  def name(name: String): FakeGaugeBuilder = this
  def namespace(namespace: String): FakeGaugeBuilder = this
  def register(): Gauge = new FakeGauge()
}
