package frankenpaxos.monitoring

// The Builder trait wraps the Prometheus Java library's Builder trait [1]. A
// builder---which is typically created by a collector---is used to build a
// metric. For example, the following code
//
//   val requestsTotal: Counter = counterCollector
//     .build()
//     .name("requests_total")
//     .labelNames("type")
//     .help("Total number of processed requests.")
//     .register()
//
// builds a Counter named "requests_total" with a label called "type".
//
// Builder uses F-bounded polymorphism, so Self is the implementing subclass.
// Metric is the type of Metric (e.g., Counter, Gauge) being returned.
//
// [1]: http://prometheus.github.io/client_java/io/prometheus/client/SimpleCollector.Builder.html
trait Builder[Self <: Builder[Self, Metric], Metric] {
  def help(help: String): Self
  def labelNames(labelNames: String*): Self
  def name(name: String): Self
  def namespace(namespace: String): Self
  def register(): Metric
}
