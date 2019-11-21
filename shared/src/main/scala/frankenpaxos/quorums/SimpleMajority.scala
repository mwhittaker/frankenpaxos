package frankenpaxos.quorums

class SimpleMajority[T](
    members: Set[T],
    seed: Long = System.currentTimeMillis()
) extends QuorumSystem[T] {
  require(
    !members.isEmpty,
    "You cannot construct a SimpleMajority quorum system without any members."
  )

  private val rand = new scala.util.Random(seed)

  private val quorumSize = members.size / 2 + 1

  override def nodes(): Set[T] = members

  override def randomReadQuorum(): Set[T] =
    rand.shuffle(members).take(quorumSize)

  override def randomWriteQuorum(): Set[T] = randomReadQuorum()

  override def isReadQuorum(nodes: Set[T]): Boolean = {
    require(
      nodes.subsetOf(members),
      s"Nodes $nodes are not a subset of this quorum system's nodes $members."
    )
    nodes.subsetOf(members) && nodes.size >= quorumSize
  }

  override def isWriteQuorum(nodes: Set[T]): Boolean = isReadQuorum(nodes)
}
