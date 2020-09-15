package frankenpaxos.multipaxos

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
    // If flexible is false, then the acceptors are organized into a number of
    // acceptor groups, and the log is round-robin partitioned between the
    // acceptor groups. If flexible is true, then the acceptors are arranged
    // into a grid. Every row is a read quorum, and every column is a write
    // quorum. The log is not partitioned.
    flexible: Boolean,
    distributionScheme: DistributionScheme
) {
  val numBatchers = batcherAddresses.size
  val numReadBatchers = readBatcherAddresses.size
  val numLeaders = leaderAddresses.size
  val numProxyLeaders = proxyLeaderAddresses.size
  val numAcceptorGroups = acceptorAddresses.size
  val numReplicas = replicaAddresses.size
  val numProxyReplicas = proxyReplicaAddresses.size

  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")

    // Batchers.
    //
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

    // Read Batchers.
    //
    // We either have no read batchers (in which case clients sends straight to
    // acceptors), or we have at least f + 1 read batchers to tolerate
    // failures.
    require(
      numReadBatchers == 0 || numReadBatchers >= f + 1,
      s"numReadBatchers must be 0 or >= f + 1 (${f + 1}). It's " +
        s"$numReadBatchers."
    )

    // Leaders.
    require(
      numLeaders >= f + 1,
      s"numLeaders must be >= f + 1 (${f + 1}). It's $numLeaders."
    )
    require(
      leaderElectionAddresses.size == numLeaders,
      s"leaderElectionAddresses.size must be equal to numLeaders " +
        s"(${numLeaders}). It's ${leaderElectionAddresses.size}."
    )

    // Proxy Leaders.
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

    // Acceptors.
    if (!flexible) {
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
    } else {
      // For simplicity, we assume that every row has the same number of
      // acceptors. This is not strictly needed, but keeps things simple.
      require(
        numAcceptorGroups >= 1,
        s"numAcceptorGroups must be >= 1. It's $numAcceptorGroups."
      )

      for (row <- acceptorAddresses) {
        require(
          row.size == acceptorAddresses(0).size,
          s"All row sizes must be the same, but one is " +
            s"${acceptorAddresses(0).size} and one is ${row.size}."
        )
      }

      // An n by m grid can tolerate min(n, m) - 1 failures.
      val n = acceptorAddresses.size
      val m = acceptorAddresses(0).size
      require(
        scala.math.min(n, m) - 1 >= f,
        s"An n x m grid can tolerate min(n, m) - 1 failures. We have a $n x " +
          s"$m grid, so it can tolerate ${scala.math.min(n, m) - 1} " +
          s"failures, but this is smaller than f = $f."
      )
    }

    // Replicas.
    require(
      numReplicas >= f + 1,
      s"numReplicas must be >= f + 1 (${f + 1}). It's $numReplicas."
    )

    // Proxy Replicas.
    require(
      numProxyReplicas == 0 || numProxyReplicas >= f + 1,
      s"numProxyReplicas must be 0 or >= f + 1 (${f + 1}). It's " +
        s"$numProxyReplicas."
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
