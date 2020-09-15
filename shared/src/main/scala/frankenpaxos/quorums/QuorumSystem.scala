package frankenpaxos.quorums

// A _quorum system_ is a set X along with a set of subsets Q of X such that
// for every p, q in Q, p and q intersect [2]. A read-write quorum system is a
// set X along with two sets of subsets R and W of X such that for every r in R
// and for every w in W, r and w intersect.
//
// Typically, MultiPaxos is described with the assumption of a simple majority
// based quorum system in which every quorum is a majority. Flexible Paxos
// popularized the idea that this assumption is sufficient but necessary.
// MultiPaxos really only requires a read-write quorum system. Here, we codify
// read-write quorum systems, but call them just `QuorumSystem`s.
//
// [1]: scholar.google.com/scholar?cluster=6385848907135897161
// [2]: scholar.google.com/scholar?cluster=9274625859195974724
trait QuorumSystem[T] {
  def nodes(): Set[T]
  def randomReadQuorum(): Set[T]
  def randomWriteQuorum(): Set[T]
  def isReadQuorum(nodes: Set[T]): Boolean
  def isWriteQuorum(nodes: Set[T]): Boolean
  def isSuperSetOfReadQuorum(nodes: Set[T]): Boolean
  def isSuperSetOfWriteQuorum(nodes: Set[T]): Boolean
}

object QuorumSystem {
  def toProto(quorumSystem: SimpleMajority[Int]): QuorumSystemProto = {
    QuorumSystemProto().withSimpleMajorityProto(
      SimpleMajorityProto(members = quorumSystem.members.toSeq)
    )
  }

  def toProto(quorumSystem: UnanimousWrites[Int]): QuorumSystemProto = {
    QuorumSystemProto().withUnanimousWritesProto(
      UnanimousWritesProto(members = quorumSystem.members.toSeq)
    )
  }

  def toProto(quorumSystem: Grid[Int]): QuorumSystemProto = {
    QuorumSystemProto().withGridProto(
      GridProto(members = quorumSystem.grid.map(row => GridRow(row)))
    )
  }

  def fromProto(
      proto: QuorumSystemProto,
      seed: Long = System.currentTimeMillis
  ): QuorumSystem[Int] = {
    import QuorumSystemProto.Value
    proto.value match {
      case Value.SimpleMajorityProto(proto) =>
        new SimpleMajority(proto.members.toSet, seed)
      case Value.UnanimousWritesProto(proto) =>
        new UnanimousWrites(proto.members.toSet, seed)
      case Value.GridProto(proto) =>
        new Grid(proto.members.map(_.xs), seed)
      case Value.Empty =>
        throw new IllegalArgumentException("Empty QuorumSystemProto.")
    }
  }
}
