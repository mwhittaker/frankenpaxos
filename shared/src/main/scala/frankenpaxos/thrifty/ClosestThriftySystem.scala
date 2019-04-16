package frankenpaxos.thrifty

object ClosestThriftySystem {
  def choose[Transport <: frankenpaxos.Transport[Transport]](
      delays: Map[Transport#Address, java.time.Duration],
      min: Int
  ): Set[Transport#Address] = {
    delays.toSeq
      .sortBy({ case (_, d) => d })
      .take(min)
      .map({ case (a, d) => a })
      .toSet
  }
}
