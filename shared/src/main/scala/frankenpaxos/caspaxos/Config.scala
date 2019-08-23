package frankenpaxos.caspaxos

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    leaderAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Transport#Address]
) {
  val quorumSize: Int = f + 1
  val n: Int = (2 * f) + 1

  def valid(): Boolean =
    (leaderAddresses.size >= f + 1) && (acceptorAddresses.size == n)
}
