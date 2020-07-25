package frankenpaxos.craq

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    batcherAddresses: Seq[Transport#Address],
    readBatcherAddresses: Seq[Transport#Address],
    leaderAddresses: Seq[Transport#Address],
    leaderElectionAddresses: Seq[Transport#Address],
    proxyLeaderAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Seq[Transport#Address]],
    replicaAddresses: Seq[Transport#Address],
    proxyReplicaAddresses: Seq[Transport#Address],
    chainNodeAddresses: Seq[Transport#Address],
    distributionScheme: DistributionScheme
) {
  val quorumSize = f + 1
  val numBatchers = batcherAddresses.size
  val numReadBatchers = readBatcherAddresses.size
  val numLeaders = leaderAddresses.size
  val numProxyLeaders = proxyLeaderAddresses.size
  val numAcceptorGroups = acceptorAddresses.size
  val numReplicas = replicaAddresses.size
  val numProxyReplicas = proxyReplicaAddresses.size
  val numChainNodes = chainNodeAddresses.size

  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")
    // We either have no batchers (in which case clients sends straight to
    // leaders), or we have at least f + 1 batchers to tolerate failures.
    distributionScheme match {
      case Hash =>
        require(
          numBatchers == 0 || numBatchers >= f + 1,
          s"numBatchers must be 0 or >= f + 1 (${f + 1}). It's $numBatchers."
        )
      case Colocated =>
        require(
          numBatchers == 0 || numBatchers == numLeaders,
          s"numBatchers must be 0 or equal to numLeaders ($numLeaders). " +
            s"It's $numBatchers."
        )
    }
  }
}
