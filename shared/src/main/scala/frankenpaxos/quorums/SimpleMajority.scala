package frankenpaxos.quorums

// A SimpleMajority quorum system with a set of nodes X is the quorum system in
// which every majority of X serves as a read and write qourum. For example,
// given nodes a, b, c, d, e, the following are the quorums:
//
//   - a, b, c
//   - a, b, d
//   - a, b, e
//   - a, c, d
//   - a, c, e
//   - a, d, e
//   - a, b, c, d
//   - a, b, c, e
//   - a, b, d, e
//   - a, c, d, e
//   - b, c, d, e
//   - a, b, c, d, e
class SimpleMajority[T](
    private[quorums] val members: Set[T],
    seed: Long = System.currentTimeMillis()
) extends QuorumSystem[T] {
  require(
    !members.isEmpty,
    "You cannot construct a SimpleMajority quorum system without any members."
  )

  private val rand = new scala.util.Random(seed)

  private val quorumSize = members.size / 2 + 1

  override def toString(): String = s"SimpleMajority(members=$members)"

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

  override def isSuperSetOfReadQuorum(nodes: Set[T]): Boolean =
    nodes.count(members.contains) >= quorumSize

  override def isSuperSetOfWriteQuorum(nodes: Set[T]): Boolean =
    isSuperSetOfReadQuorum(nodes)
}
