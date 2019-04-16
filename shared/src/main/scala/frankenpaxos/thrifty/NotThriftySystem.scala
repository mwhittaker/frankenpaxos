package frankenpaxos.thrifty

object NotThriftySystem extends ThriftySystem {
  def choose[Transport <: frankenpaxos.Transport[Transport]](
      delays: Map[Transport#Address, java.time.Duration],
      min: Int
  ): Set[Transport#Address] = delays.keySet
}
