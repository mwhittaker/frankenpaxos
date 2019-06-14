package frankenpaxos.monitoring

import collection.SortedMap
import collection.mutable

object FakeSummaryCollector extends Collector[FakeSummaryBuilder, Summary] {
  override def build(): Builder[FakeSummaryBuilder, Summary] =
    new FakeSummaryBuilder()
}

// TODO(mwhittaker): Incorporate builder information into FakeSummary.
class FakeSummaryBuilder extends SummaryBuilder[FakeSummaryBuilder] {
  override def help(help: String): FakeSummaryBuilder = this
  override def labelNames(labelNames: String*): FakeSummaryBuilder = this
  override def name(name: String): FakeSummaryBuilder = this
  override def namespace(namespace: String): FakeSummaryBuilder = this
  override def register(): Summary = new FakeSummary()
  override def quantile(quantile: Double, error: Double): FakeSummaryBuilder =
    this
}

class FakeSummaryImpl extends SummaryImpl {
  var count: Double = 0
  var sum: Double = 0

  override def get(): SummaryValue =
    SummaryValue(
      count = count,
      quantiles = SortedMap[Double, Double](),
      sum = sum
    )

  override def observe(amt: Double): Unit = {
    count += 1
    sum += amt
  }
}

class FakeSummary extends Summary {
  var impls = mutable.Map[Seq[String], FakeSummaryImpl]()
  var noLabelsSummary = new FakeSummaryImpl()

  override type Impl = FakeSummaryImpl
  override def labels(labelValues: String*): Impl =
    impls.getOrElseUpdate(labelValues, new FakeSummaryImpl())
  override def get(): SummaryValue = noLabelsSummary.get()
  override def observe(amt: Double): Unit = noLabelsSummary.observe(amt)
}
