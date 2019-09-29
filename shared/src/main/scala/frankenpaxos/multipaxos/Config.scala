package frankenpaxos.multipaxos

import frankenpaxos.roundsystem.RoundSystem

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    batcherAddresses: Seq[Transport#Address],
    leaderAddresses: Seq[Transport#Address],
    proxyLeaderAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Seq[Transport#Address]],
    replicaAddresses: Seq[Transport#Address],
    proxyReplicaAddresses: Seq[Transport#Address],
    roundSystem: RoundSystem
) {
  val quorumSize = f + 1
  val numBatchers = batcherAddresses.size
  val numLeaders = leaderAddresses.size
  val numProxyLeaders = proxyLeaderAddresses.size
  val numAcceptorGroups = acceptorAddresses.size
  val numReplicas = replicaAddresses.size
  val numProxyReplicas = proxyReplicaAddresses.size

  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")
    // We either have no batchers (in which case clients sends straight to
    // leaders), or we have at least f + 1 batchers to tolerate failures.
    require(
      numBatchers == 0 || numBatchers >= f + 1,
      s"numBatchers must be >= f + 1 (${f + 1}). It's ${numBatchers}."
    )
    require(
      numLeaders >= f + 1,
      s"numLeaders must be >= f + 1 (${f + 1}). It's ${numLeaders}."
    )
    // We either have no proxy leaders (in which case leaders act as proxy
    // leaders), or we have at least f + 1 proxy leaders to tolerate failures.
    require(
      numProxyLeaders == 0 || numProxyLeaders >= f + 1,
      s"numProxyLeaders must be >= f + 1 (${f + 1}). It's ${numProxyLeaders}."
    )
    require(
      numAcceptorGroups >= 1,
      s"numAcceptorGroups must be >= 1. It's ${numAcceptorGroups}."
    )
    for (acceptorCluster <- acceptorAddresses) {
      require(
        acceptorCluster.size == 2 * f + 1,
        s"acceptorCluster.size must be 2*f + 1 (${2 * f + 1}). " +
          s"It's ${acceptorCluster.size}."
      )
    }
    require(
      numReplicas >= f + 1,
      s"numReplicas must be >= f + 1 (${f + 1}). It's ${numReplicas}."
    )
    require(
      numProxyReplicas >= f + 1,
      s"numProxyReplicas must be >= f + 1 (${f + 1}). It's ${numProxyReplicas}."
    )
  }
}
