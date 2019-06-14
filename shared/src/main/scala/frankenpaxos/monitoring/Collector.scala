package frankenpaxos.monitoring

// The Collector trait wraps the Prometheus Java library's SimpleCollector
// trait. A collector is pretty much used to make a Builder. See Builder for an
// example.
//
// B is a Builder, and M is a metric.
//
// [1]: http://prometheus.github.io/client_java/io/prometheus/client/SimpleCollector.html
trait Collector[B <: Builder[B, M], M] {
  def build(): Builder[B, M]
}
