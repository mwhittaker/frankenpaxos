package frankenpaxos.spaxosdecouple
import frankenpaxos.roundsystem.RoundSystem

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    proposerAddresses: Seq[Transport#Address],
    disseminatorAddresses: Seq[Transport#Address],
    leaderAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Transport#Address],
    acceptorHeartbeatAddresses: Seq[Transport#Address],
    leaderHeartbeatAddresses: Seq[Transport#Address],
    leaderElectionAddresses: Seq[Transport#Address],
    executorAddresses: Seq[Transport#Address],
    roundSystem: RoundSystem
) {
  def n: Int = (2 * f) + 1

  def quorumSize = f + 1

  def valid(): Boolean = {
    return true//replicaAddresses.size == f + 1
  }
}
