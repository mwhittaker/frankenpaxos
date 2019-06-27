package frankenpaxos.unanimousbpaxos

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    leaderAddresses: Seq[Transport#Address],
    depServiceNodeAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Transport#Address]
) {
  def n: Int = (2 * f) + 1
  def classicQuorumSize = f + 1
  def fastQuorumSize = n

  def valid(): Boolean = {
    return (leaderAddresses.size == f + 1) &&
      (depServiceNodeAddresses.size == n) &&
      (acceptorAddresses.size == n)
  }
}
