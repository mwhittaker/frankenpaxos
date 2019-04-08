package frankenpaxos.monitoring

import collection.mutable

class FakeCounterImpl extends CounterImpl {
  var count: Double = 0
  def get(): Double = count
  def inc(): Unit = count += 1
  def inc(amt: Double): Unit = count += amt
}

class FakeCounter extends Counter {
  var impls = mutable.Map[Seq[String], FakeCounterImpl]()
  var noLabelsCounter = new FakeCounterImpl()

  type Impl = FakeCounterImpl
  def labels(labelValues: String*): Impl =
    impls.getOrElseUpdate(labelValues, new FakeCounterImpl())
  def get(): Double = noLabelsCounter.get()
  def inc(): Unit = noLabelsCounter.inc()
  def inc(amt: Double): Unit = noLabelsCounter.inc(amt)
}
