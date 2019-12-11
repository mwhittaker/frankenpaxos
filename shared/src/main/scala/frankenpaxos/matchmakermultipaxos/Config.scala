package frankenpaxos.matchmakermultipaxos

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    leaderAddresses: Seq[Transport#Address],
    leaderElectionAddresses: Seq[Transport#Address],
    reconfigurerAddresses: Seq[Transport#Address],
    // As with acceptorAddresses, we don't _need_ a fixed set of matchmakers,
    // but we assume it to make things simpler.
    matchmakerAddresses: Seq[Transport#Address],
    // Matchmaker Paxos doesn't require a fixed pre-determined set of
    // acceptors. A leader is free to select _any_ set of acceptors that it
    // pleases. To keep things simple, here we fix a set of acceptors and have
    // each leader pick a random subset of them. This is not fundamental, just
    // a simplification.
    acceptorAddresses: Seq[Transport#Address],
    replicaAddresses: Seq[Transport#Address]
) {
  val quorumSize = f + 1
  val numLeaders = leaderAddresses.size
  val numReconfigurers = reconfigurerAddresses.size
  val numMatchmakers = matchmakerAddresses.size
  val numAcceptors = acceptorAddresses.size
  val numReplicas = replicaAddresses.size

  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")
    require(
      numLeaders >= f + 1,
      s"numLeaders must be >= f + 1 (${f + 1}). It's $numLeaders."
    )
    require(
      leaderElectionAddresses.size == numLeaders,
      s"The number of election addresses must be the same as the number of " +
        s"leaders ($numLeaders). It's ${leaderElectionAddresses.size}."
    )
    require(
      numReconfigurers >= f + 1,
      s"numReconfigurers must be >= f + 1 (${f + 1}). It's $numReconfigurers."
    )
    require(
      numMatchmakers >= 2 * f + 1,
      s"numMatchmakers must be >= 2*f + 1 (${2 * f + 1}). It's $numMatchmakers."
    )
    require(
      numAcceptors >= f + 1,
      s"numAcceptors must be >= f+1 (${f + 1}). It's $numAcceptors."
    )
    require(
      numReplicas >= 2 * f + 1,
      s"numReplicas must be >= 2*f+1 (${2 * f + 1}). It's $numReplicas."
    )
  }
}
