package frankenpaxos.quorums

class UnanimousWrites[T](members: Set[T], seed: Long = System.currentTimeMillis)
    extends QuorumSystem[T] {
  require(
    !members.isEmpty,
    "You cannot construct a SimpleMajority quorum system without any members."
  )

  private val rand = new scala.util.Random(seed)

  override def nodes(): Set[T] = members

  override def randomReadQuorum(): Set[T] = rand.shuffle(members).take(1)

  override def randomWriteQuorum(): Set[T] = members

  override def isReadQuorum(nodes: Set[T]): Boolean = {
    require(
      nodes.subsetOf(members),
      s"Nodes $nodes are not a subset of this quorum system's nodes $members."
    )
    !nodes.isEmpty
  }

  override def isWriteQuorum(nodes: Set[T]): Boolean = {
    require(
      nodes.subsetOf(members),
      s"Nodes $nodes are not a subset of this quorum system's nodes $members."
    )

    nodes == members
  }
}
