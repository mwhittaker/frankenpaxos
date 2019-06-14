package frankenpaxos.monitoring

import collection.mutable

object FakeCounterCollector extends Collector[FakeCounterBuilder, Counter] {
  override def build(): Builder[FakeCounterBuilder, Counter] =
    new FakeCounterBuilder()
}

// TODO(mwhittaker): Incorporate builder information into FakeCounter.
class FakeCounterBuilder extends CounterBuilder[FakeCounterBuilder] {
  override def help(help: String): FakeCounterBuilder = this
  override def labelNames(labelNames: String*): FakeCounterBuilder = this
  override def name(name: String): FakeCounterBuilder = this
  override def namespace(namespace: String): FakeCounterBuilder = this
  override def register(): Counter = new FakeCounter()
}

class FakeCounterImpl extends CounterImpl {
  var count: Double = 0
  override def get(): Double = count
  override def inc(): Unit = count += 1
  override def inc(amt: Double): Unit = count += amt
}

class FakeCounter extends Counter {
  var impls = mutable.Map[Seq[String], FakeCounterImpl]()
  var noLabelsCounter = new FakeCounterImpl()

  override type Impl = FakeCounterImpl
  override def labels(labelValues: String*): Impl =
    impls.getOrElseUpdate(labelValues, new FakeCounterImpl())
  override def get(): Double = noLabelsCounter.get()
  override def inc(): Unit = noLabelsCounter.inc()
  override def inc(amt: Double): Unit = noLabelsCounter.inc(amt)
}
