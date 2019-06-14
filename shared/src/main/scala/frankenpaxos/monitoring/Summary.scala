package frankenpaxos.monitoring

import scala.collection.SortedMap

// The Summary trait mimics the Prometheus Java library's Summary class, and
// SummaryImpl mimics the Summary.Child class [1]. SummaryValue mimics
// Summary.Child.Value.
//
// [1]: http://prometheus.github.io/client_java/io/prometheus/client/Summary.html

trait SummaryBuilder[Self <: SummaryBuilder[Self]]
    extends Builder[Self, Summary] {
  def quantile(quantile: Double, error: Double): Self
}

case class SummaryValue(
    count: Double,
    quantiles: SortedMap[Double, Double],
    sum: Double
)

trait SummaryImpl {
  def get(): SummaryValue
  def observe(amt: Double): Unit
}

trait Summary extends SummaryImpl {
  type Impl <: SummaryImpl
  def labels(labelValues: String*): Impl
  def get(): SummaryValue
  def observe(amt: Double): Unit
}
