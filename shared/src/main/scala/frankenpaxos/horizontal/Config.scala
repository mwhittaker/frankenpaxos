package frankenpaxos.horizontal

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    leaderAddresses: Seq[Transport#Address],
    leaderElectionAddresses: Seq[Transport#Address],
    // Horizontal MultiPaxos doesn't require a fixed pre-determined set of
    // acceptors. To keep things simple, here we fix a set of acceptors and
    // have each leader pick a random subset of them. This is not fundamental,
    // just a simplification.
    acceptorAddresses: Seq[Transport#Address],
    replicaAddresses: Seq[Transport#Address]
) {
  val numLeaders = leaderAddresses.size
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
      numAcceptors >= 2 * f + 1,
      s"numAcceptors must be >= 2*f+1 (${2 * f + 1}). It's $numAcceptors."
    )
    require(
      numReplicas >= f + 1,
      s"numReplicas must be >= 2*f+1 (${f + 1}). It's $numReplicas."
    )
  }
}
