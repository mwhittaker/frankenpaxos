package frankenpaxos.monitoring

import io.prometheus

object PrometheusCounterCollector
    extends Collector[PrometheusCounterBuilder, Counter] {
  override def build(): Builder[PrometheusCounterBuilder, Counter] =
    new PrometheusCounterBuilder(prometheus.client.Counter.build())
}

class PrometheusCounterBuilder(builder: prometheus.client.Counter.Builder)
    extends CounterBuilder[PrometheusCounterBuilder] {
  override def help(help: String): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.help(help))

  override def labelNames(labelNames: String*): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.labelNames(labelNames: _*))

  override def name(name: String): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.name(name))

  override def namespace(namespace: String): PrometheusCounterBuilder =
    new PrometheusCounterBuilder(builder.namespace(namespace))

  override def register(): Counter =
    new PrometheusCounter(builder.register())
}

class PrometheusCounterImpl(child: prometheus.client.Counter.Child)
    extends CounterImpl {
  override def get(): Double = child.get()
  override def inc(): Unit = child.inc()
  override def inc(amt: Double): Unit = child.inc(amt)
}

class PrometheusCounter(counter: prometheus.client.Counter) extends Counter {
  override type Impl = PrometheusCounterImpl
  override def labels(labelValues: String*): Impl =
    new PrometheusCounterImpl(counter.labels(labelValues: _*))
  override def get(): Double = counter.get()
  override def inc(): Unit = counter.inc()
  override def inc(amt: Double): Unit = counter.inc(amt)
}
