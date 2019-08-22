package frankenpaxos.spaxos

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    replicaAddresses: Seq[Transport#Address]
) {
  def n: Int = (2 * f) + 1

  def quorumSize = f + 1

  def valid(): Boolean = {
    return replicaAddresses.size == f + 1
  }
}
