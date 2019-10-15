package frankenpaxos.mencius

import scala.scalajs.js.annotation.JSExportAll

// - any number of clients
// - 0 or >=f+1 batchers
// - >=1 group of >=f+1 leaders
// - one set of >=1 group of 2f+1 acceptors per leader group
// - >=f+1 replicas
// - >=f+1 proxy replicas
//
// (c) | (b) | (l)(l) | (p) | (a)(a)(a) | (r) | (p) |
//     |     |        |     | (a)(a)(a) |     |     |
//     |     |--------|     |-----------|     |     |
// (c) | (b) | (l)(l) | (p) | (a)(a)(a) | (r) | (p) |
//     |     |        |     | (a)(a)(a) |     |     |
//     |     |--------|     |-----------|     |     |
// (c) | (b) | (l)(l) | (p) | (a)(a)(a) | (r) | (p) |
//     |     |        |     | (a)(a)(a) |     |     |
@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    batcherAddresses: Seq[Transport#Address],
    leaderAddresses: Seq[Seq[Transport#Address]],
    leaderElectionAddresses: Seq[Seq[Transport#Address]],
    proxyLeaderAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Seq[Seq[Transport#Address]]],
    replicaAddresses: Seq[Transport#Address],
    proxyReplicaAddresses: Seq[Transport#Address],
    distributionScheme: DistributionScheme
) {
  val quorumSize = f + 1
  val numBatchers = batcherAddresses.size
  val numLeaderGroups = leaderAddresses.size
  val numProxyLeaders = proxyLeaderAddresses.size
  val numReplicas = replicaAddresses.size
  val numProxyReplicas = proxyReplicaAddresses.size

  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")
    // We either have no batchers (in which case clients sends straight to
    // leaders), or we have at least f + 1 batchers to tolerate failures.
    require(
      numBatchers == 0 || numBatchers >= f + 1,
      s"numBatchers must be 0 or >= f + 1 (${f + 1}). It's $numBatchers."
    )

    // We must have at least one leader group and at least f+1 leaders per
    // group. Leader election addresses should mirror leaders exactly.
    require(
      numLeaderGroups >= 1,
      s"numLeaderGroups must be >= 1. It's $numLeaderGroups."
    )
    for ((group, i) <- leaderAddresses.zipWithIndex) {
      require(
        group.size >= f + 1,
        s"Leader group $i size must be >= f + 1 (${f + 1}). It's ${group.size}."
      )
    }
    require(
      leaderElectionAddresses.size == numLeaderGroups,
      s"leaderElectionAddresses.size must be equal to numLeaderGroups " +
        s"(${numLeaderGroups}). It's ${leaderElectionAddresses.size}."
    )
    for ((group, i) <- leaderElectionAddresses.zipWithIndex) {
      require(
        group.size == leaderAddresses(i).size,
        s"Leader election group $i size must be == leader group $i size " +
          s"(${leaderAddresses(i).size}). It's ${group.size}."
      )
    }

    // We must have at least f + 1 proxy leaders. If we're colocating the
    // leaders and proxy leaders, then every active leader must be co-located
    // with a proxy leader. The non-active leaders don't get co-located, but
    // that's okay since we don't see failures in practice.
    require(
      numProxyLeaders >= f + 1,
      s"numProxyLeaders must be >= f + 1 (${f + 1}). It's $numProxyLeaders."
    )
    if (distributionScheme == Colocated) {
      require(
        numProxyLeaders == numLeaderGroups,
        s"numProxyLeaders must equal numLeaderGroups ($numLeaderGroups). " +
          s"It's $numProxyLeaders."
      )
    }

    // The number of acceptor group groups must be equal to the number of
    // leader groups since we have one group of acceptor groups per leader
    // group.
    require(
      acceptorAddresses.size == numLeaderGroups,
      s"The number of acceptor group groups must equal the number of leader " +
        s"groups (${numLeaderGroups}). It's ${acceptorAddresses.size}."
    )
    for ((groups, i) <- acceptorAddresses.zipWithIndex) {
      require(
        groups.size >= 1,
        s"Acceptor group group $i size must be >= 1. It's ${groups.size}."
      )

      for ((group, j) <- groups.zipWithIndex) {
        require(
          group.size == 2 * f + 1,
          s"Acceptor group $i.$j size must be 2*f + 1 (${2 * f + 1}). " +
            s"It's ${group.size}."
        )
      }
    }

    // We must have at least f + 1 replicas and proxy replicas, and if we're
    // co-locating, then they better be equal.
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
