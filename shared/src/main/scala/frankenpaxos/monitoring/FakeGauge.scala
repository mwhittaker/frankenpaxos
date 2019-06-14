package frankenpaxos.monitoring

import collection.mutable

object FakeGaugeCollector extends Collector[FakeGaugeBuilder, Gauge] {
  override def build(): Builder[FakeGaugeBuilder, Gauge] =
    new FakeGaugeBuilder()
}

// TODO(mwhittaker): Incorporate builder information into FakeGauge.
class FakeGaugeBuilder extends GaugeBuilder[FakeGaugeBuilder] {
  override def help(help: String): FakeGaugeBuilder = this
  override def labelNames(labelNames: String*): FakeGaugeBuilder = this
  override def name(name: String): FakeGaugeBuilder = this
  override def namespace(namespace: String): FakeGaugeBuilder = this
  override def register(): Gauge = new FakeGauge()
}

class FakeGaugeImpl extends GaugeImpl {
  var count: Double = 0
  override def get(): Double = count
  override def set(x: Double): Unit = count = x
  override def inc(): Unit = count += 1
  override def inc(amt: Double): Unit = count += amt
  override def dec(): Unit = count -= 1
  override def dec(amt: Double): Unit = count -= amt
}

class FakeGauge extends Gauge {
  var impls = mutable.Map[Seq[String], FakeGaugeImpl]()
  var noLabelsGauge = new FakeGaugeImpl()

  override type Impl = FakeGaugeImpl
  override def labels(labelValues: String*): Impl =
    impls.getOrElseUpdate(labelValues, new FakeGaugeImpl())
  override def get(): Double = noLabelsGauge.get()
  override def set(x: Double): Unit = noLabelsGauge.set(x)
  override def inc(): Unit = noLabelsGauge.inc()
  override def inc(amt: Double): Unit = noLabelsGauge.inc(amt)
  override def dec(): Unit = noLabelsGauge.dec()
  override def dec(amt: Double): Unit = noLabelsGauge.dec(amt)
}
