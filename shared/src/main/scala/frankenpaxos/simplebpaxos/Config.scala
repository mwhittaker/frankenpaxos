package frankenpaxos.simplebpaxos

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    leaderAddresses: Seq[Transport#Address],
    depServiceNodeAddresses: Seq[Transport#Address],
    proposerAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Transport#Address],
    replicaAddresses: Seq[Transport#Address]
) {
  def n: Int = (2 * f) + 1

  def quorumSize = f + 1

  def valid(): Boolean = {
    return (leaderAddresses.size >= f + 1) &&
      (depServiceNodeAddresses.size == n) &&
      (proposerAddresses.size == leaderAddresses.size) &&
      (acceptorAddresses.size == n) &&
      (replicaAddresses.size == f + 1)
  }
}
