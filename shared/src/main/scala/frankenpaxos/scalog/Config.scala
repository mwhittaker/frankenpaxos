package frankenpaxos.scalog

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    serverAddresses: Seq[Seq[Transport#Address]],
    aggregatorAddress: Transport#Address,
    leaderAddresses: Seq[Transport#Address],
    leaderElectionAddresses: Seq[Transport#Address],
    acceptorAddresses: Seq[Transport#Address],
    replicaAddresses: Seq[Transport#Address]
) {
  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")

    // Servers.
    require(serverAddresses.size > 0, "There must be at least one shard.")
    for ((shard, i) <- serverAddresses.zipWithIndex) {
      require(
        shard.size >= f + 1,
        s"There must be at least f + 1 (${f + 1}) servers in every shard, " +
          s"but the ${i}th shard has ${shard.size} servers."
      )
    }
    require(
      serverAddresses.forall(_.size == serverAddresses(0).size),
      s"For simplicity, every shard must have the same number of servers, " +
        s"but the shards have ${serverAddresses.map(_.size)} servers."
    )

    // Leaders.
    require(
      leaderAddresses.size == f + 1,
      s"There must be f + 1 (${f + 1}) leaders, but there's " +
        s"${leaderAddresses.size}."
    )
    require(
      leaderElectionAddresses.size == leaderAddresses.size,
      s"There must be ${leaderAddresses.size} leader election addresses, " +
        s"but there's ${leaderElectionAddresses.size}."
    )

    // Acceptors.
    require(
      acceptorAddresses.size == 2 * f + 1,
      s"There must be 2f + 1 (${2 * f + 1}) acceptors, but there's " +
        s"${acceptorAddresses.size}."
    )

    // Replicas.
    require(
      replicaAddresses.size == f + 1,
      s"There must be f + 1 (${f + 1}) acceptors, but there's " +
        s"${replicaAddresses.size}."
    )
  }
}
