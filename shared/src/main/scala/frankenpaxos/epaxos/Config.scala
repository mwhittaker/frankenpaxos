package frankenpaxos.epaxos

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    replicaAddresses: Seq[Transport#Address],
) {
  def n: Int = (2 * f) + 1

  def valid(): Boolean = {
    replicaAddresses.size >= f + 1
  }
}
