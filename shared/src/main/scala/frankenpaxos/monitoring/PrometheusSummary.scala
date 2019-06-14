package frankenpaxos.monitoring

import collection.JavaConverters._
import io.prometheus
import scala.collection.SortedMap
import scala.collection.mutable

object PrometheusSummaryCollector
    extends Collector[PrometheusSummaryBuilder, Summary] {
  override def build(): Builder[PrometheusSummaryBuilder, Summary] =
    new PrometheusSummaryBuilder(prometheus.client.Summary.build())
}

class PrometheusSummaryBuilder(builder: prometheus.client.Summary.Builder)
    extends SummaryBuilder[PrometheusSummaryBuilder] {
  override def help(help: String): PrometheusSummaryBuilder =
    new PrometheusSummaryBuilder(builder.help(help))

  override def labelNames(labelNames: String*): PrometheusSummaryBuilder =
    new PrometheusSummaryBuilder(builder.labelNames(labelNames: _*))

  override def name(name: String): PrometheusSummaryBuilder =
    new PrometheusSummaryBuilder(builder.name(name))

  override def namespace(namespace: String): PrometheusSummaryBuilder =
    new PrometheusSummaryBuilder(builder.namespace(namespace))

  override def register(): Summary =
    new PrometheusSummary(builder.register())

  override def quantile(
      quantile: Double,
      error: Double
  ): PrometheusSummaryBuilder =
    new PrometheusSummaryBuilder(builder.quantile(quantile, error))
}

class PrometheusSummaryImpl(child: prometheus.client.Summary.Child)
    extends SummaryImpl {
  override def get(): SummaryValue = {
    val value = child.get()
    val quantiles: mutable.Map[Double, Double] = value.quantiles.asScala.map({
      case (k, v) => (k.doubleValue, v.doubleValue)
    })
    SummaryValue(
      count = value.count,
      quantiles = SortedMap[Double, Double]() ++ quantiles,
      sum = value.sum
    )
  }

  override def observe(amt: Double): Unit = child.observe(amt)
}

class PrometheusSummary(summary: prometheus.client.Summary) extends Summary {
  override type Impl = PrometheusSummaryImpl

  override def labels(labelValues: String*): Impl =
    new PrometheusSummaryImpl(summary.labels(labelValues: _*))

  override def get(): SummaryValue = {
    val value = summary.get()
    val quantiles: mutable.Map[Double, Double] = value.quantiles.asScala.map({
      case (k, v) => (k.doubleValue, v.doubleValue)
    })
    SummaryValue(
      count = value.count,
      quantiles = SortedMap[Double, Double]() ++ quantiles,
      sum = value.sum
    )
  }

  override def observe(amt: Double): Unit = summary.observe(amt)
}
