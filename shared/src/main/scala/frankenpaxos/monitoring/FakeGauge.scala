package frankenpaxos.monitoring

import collection.mutable

class FakeGaugeImpl extends GaugeImpl {
  var count: Double = 0
  def get(): Double = count
  def set(x: Double): Unit = count = x
  def inc(): Unit = count += 1
  def inc(amt: Double): Unit = count += amt
  def dec(): Unit = count -= 1
  def dec(amt: Double): Unit = count -= amt
}

class FakeGauge extends Gauge {
  var impls = mutable.Map[Seq[String], FakeGaugeImpl]()
  var noLabelsGauge = new FakeGaugeImpl()

  type Impl = FakeGaugeImpl
  def labels(labelValues: String*): Impl =
    impls.getOrElseUpdate(labelValues, new FakeGaugeImpl())
  def get(): Double = noLabelsGauge.get()
  def set(x: Double): Unit = noLabelsGauge.set(x)
  def inc(): Unit = noLabelsGauge.inc()
  def inc(amt: Double): Unit = noLabelsGauge.inc(amt)
  def dec(): Unit = noLabelsGauge.dec()
  def dec(amt: Double): Unit = noLabelsGauge.dec(amt)
}
