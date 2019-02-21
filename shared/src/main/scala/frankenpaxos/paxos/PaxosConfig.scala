package frankenpaxos.paxos

case class PaxosConfig[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    proposerAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Transport#Address]
) {
  def n: Int = (2 * f) + 1

  def valid(): Boolean = {
    return (proposerAddresses.size >= f + 1) && (acceptorAddresses.size == n)
  }
}
