package frankenpaxos.spaxosdecouple

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    replicaAddresses: Seq[Transport#Address],
    proposerAddresses: Seq[Transport#Address],
    disseminatorAddresses: Seq[Transport#Address],
    leaderAddresses: Seq[Transport#Address]
) {
  def n: Int = (2 * f) + 1

  def quorumSize = f + 1

  def valid(): Boolean = {
    return true//replicaAddresses.size == f + 1
  }
}
