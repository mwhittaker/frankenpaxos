package frankenpaxos.monitoring

trait Builder[Self <: Builder[Self, Metric], Metric] {
  def help(help: String): Self
  def labelNames(labelNames: String*): Self
  def name(name: String): Self
  def namespace(namespace: String): Self
  def register(): Metric
}
