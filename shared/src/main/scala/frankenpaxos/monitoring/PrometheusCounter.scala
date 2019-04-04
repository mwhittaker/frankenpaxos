package frankenpaxos.monitoring

import io.prometheus

class PrometheusCounter(counter: prometheus.client.Counter) extends Counter {
  def get(): Double = counter.get()
  def inc(): Unit = counter.inc()
  def inc(amt: Double): Unit = counter.inc(amt)
}
