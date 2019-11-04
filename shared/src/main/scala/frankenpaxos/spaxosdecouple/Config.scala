package frankenpaxos.spaxosdecouple

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    batcherAddresses: Seq[Transport#Address],
    proposerAddresses: Seq[Transport#Address],
    leaderAddresses: Seq[Transport#Address],
    leaderElectionAddresses: Seq[Transport#Address],
    proxyLeaderAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Seq[Transport#Address]],
    disseminatorAddresses: Seq[Seq[Transport#Address]],
    replicaAddresses: Seq[Transport#Address],
    proxyReplicaAddresses: Seq[Transport#Address],
    distributionScheme: DistributionScheme
) {
  val quorumSize = f + 1
  val numBatchers = batcherAddresses.size
  val numProposers = proposerAddresses.size
  val numLeaders = leaderAddresses.size
  val numProxyLeaders = proxyLeaderAddresses.size
  val numAcceptorGroups = acceptorAddresses.size
  val numReplicas = replicaAddresses.size
  val numProxyReplicas = proxyReplicaAddresses.size
  val numDisseminatorGroups = disseminatorAddresses.size

  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")
    // We either have no batchers (in which case clients sends straight to
    // leaders), or we have at least f + 1 batchers to tolerate failures.
    require(
      numBatchers == 0 || numBatchers >= f + 1,
      s"numBatchers must be 0 or >= f + 1 (${f + 1}). It's $numBatchers."
    )
    require(
      numProposers >= f + 1,
      s"numProposers must be >= f + 1 (${f + 1}). It's $numProposers."
    )
    require(
      numLeaders >= f + 1,
      s"numLeaders must be >= f + 1 (${f + 1}). It's $numLeaders."
    )
    require(
      leaderElectionAddresses.size == numLeaders,
      s"leaderElectionAddresses.size must be equal to numLeaders " +
        s"(${numLeaders}). It's ${leaderElectionAddresses.size}."
    )
    require(
      numProxyLeaders >= f + 1,
      s"numProxyLeaders must be >= f + 1 (${f + 1}). It's $numProxyLeaders."
    )
    if (distributionScheme == Colocated) {
      require(
        numProxyLeaders == numLeaders,
        s"numProxyLeaders must equal numLeaders ($numLeaders). " +
          s"It's $numProxyLeaders."
      )
    }
    require(
      numAcceptorGroups >= 1,
      s"numAcceptorGroups must be >= 1. It's $numAcceptorGroups."
    )
    for (acceptorCluster <- acceptorAddresses) {
      require(
        acceptorCluster.size == 2 * f + 1,
        s"acceptorCluster.size must be 2*f + 1 (${2 * f + 1}). " +
          s"It's ${acceptorCluster.size}."
      )
    }
    require(
      numDisseminatorGroups >= 1,
      s"numDisseminatorGroups must be >= 1. It's $numDisseminatorGroups."
    )
    for (disseminatorCluster <- disseminatorAddresses) {
      require(
        disseminatorCluster.size == 2 * f + 1,
        s"disseminatorCluster.size must be 2*f + 1 (${2 * f + 1}). " +
          s"It's ${disseminatorCluster.size}."
      )
    }
    require(
      numReplicas >= f + 1,
      s"numReplicas must be >= f + 1 (${f + 1}). It's $numReplicas."
    )
    require(
      numProxyReplicas >= f + 1,
      s"numProxyReplicas must be >= f + 1 (${f + 1}). It's $numProxyReplicas."
    )
    if (distributionScheme == Colocated) {
      require(
        numProxyReplicas == numReplicas,
        s"numProxyReplicas must equal numReplicas ($numReplicas). " +
          s"It's $numProxyReplicas."
      )
    }
  }
}
