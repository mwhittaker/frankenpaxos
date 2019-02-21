package frankenpaxos.multipaxos

case class MultiPaxosConfig[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    replicaAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Transport#Address],
    leaderAddresses: Seq[Transport#Address]
) {
  def n: Int = (2 * f) + 1

  def valid(): Boolean = {
    return (replicaAddresses.size >= f + 1) && (acceptorAddresses.size == n) && (leaderAddresses.size >= f + 1)
  }
}
