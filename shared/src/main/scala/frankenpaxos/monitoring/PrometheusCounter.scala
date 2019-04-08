package frankenpaxos.monitoring

import io.prometheus

class PrometheusCounterImpl(child: prometheus.client.Counter.Child)
    extends CounterImpl {
  def get(): Double = child.get()
  def inc(): Unit = child.inc()
  def inc(amt: Double): Unit = child.inc(amt)
}

class PrometheusCounter(counter: prometheus.client.Counter) extends Counter {
  type Impl = PrometheusCounterImpl
  def labels(labelValues: String*): Impl =
    new PrometheusCounterImpl(counter.labels(labelValues: _*))
  def get(): Double = counter.get()
  def inc(): Unit = counter.inc()
  def inc(amt: Double): Unit = counter.inc(amt)
}
