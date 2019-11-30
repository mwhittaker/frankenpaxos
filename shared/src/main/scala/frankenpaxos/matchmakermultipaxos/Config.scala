package frankenpaxos.matchmakermultipaxos

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    leaderAddresses: Seq[Transport#Address],
    // TODO(mwhittaker): When we implement matchmaker reconfiguration, we'll
    // probably do something similar to what we did with the acceptors.
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
  val numMatchmakers = matchmakerAddresses.size
  val numAcceptors = acceptorAddresses.size
  val numReplicas = replicaAddresses.size

  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")
    require(
      numLeaders >= f + 1,
      s"numLeaders must be >= f + 1 (${f + 1}). It's $numLeaders."
    )
    // TODO(mwhittaker): We'll have to update this to >= 2*f+1 after we
    // introduce matchmaker reconfiguration.
    require(
      numMatchmakers == 2 * f + 1,
      s"numMatchmakers must be 2*f + 1 (${2 * f + 1}). It's $numMatchmakers."
    )
    require(
      numAcceptors >= f + 1,
      s"numAcceptors must be >= f+1 (${f + 1}). It's $numAcceptors."
    )
    // TODO(mwhittaker): After we introduce GC and replica stabilization, we'll
    // have to change this to >= 2*f+1.
    require(
      numReplicas >= f + 1,
      s"numReplicas must be >= f+1 (${f + 1}). It's $numReplicas."
    )
  }
}
