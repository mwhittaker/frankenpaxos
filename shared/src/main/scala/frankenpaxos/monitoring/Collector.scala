package frankenpaxos.monitoring

trait Collector[B <: Builder[B, M], M] {
  def build(): Builder[B, M]
}
