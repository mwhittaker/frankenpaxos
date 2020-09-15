package frankenpaxos.quorums

// A UnanimousWrites quorum system with a set of nodes X is the quorum system
// with a single write quorum, X, and with every non-empty subset of X acting
// as a read quroum. For example, given nodes a, b, c, these are the read and
// write quorums:
//
//   Write Quorums    Read Quorums
//   =============    ============
//   a, b, c          a
//                    b
//                    c
//                    a, b
//                    a, c
//                    b, c
//                    a, b, c
class UnanimousWrites[T](
    private[quorums] val members: Set[T],
    seed: Long = System.currentTimeMillis
) extends QuorumSystem[T] {
  require(
    !members.isEmpty,
    "You cannot construct a UnanimousWrites quorum system without any members."
  )

  private val rand = new scala.util.Random(seed)

  override def toString(): String = s"UnanimousWrites(members=$members)"

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

  override def isSuperSetOfReadQuorum(nodes: Set[T]): Boolean =
    nodes.exists(members.contains)

  override def isSuperSetOfWriteQuorum(nodes: Set[T]): Boolean =
    members.subsetOf(nodes)
}
