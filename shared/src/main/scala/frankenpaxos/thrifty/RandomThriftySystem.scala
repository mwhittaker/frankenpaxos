package frankenpaxos.thrifty

object RandomThriftySystem extends ThriftySystem {
  def choose[Transport <: frankenpaxos.Transport[Transport]](
      delays: Map[Transport#Address, java.time.Duration],
      min: Int
  ): Set[Transport#Address] = scala.util.Random.shuffle(delays.keySet).take(min)
}
