package frankenpaxos.quorums

// A Grid quorum system arranges nodes into an n x m grid. Every row is a read
// quorum, and one entry from every row is a write column.
class Grid[T](
    private[quorums] val grid: Seq[Seq[T]],
    seed: Long = System.currentTimeMillis
) extends QuorumSystem[T] {
  require(
    !grid.isEmpty,
    "You cannot construct a Grid quorum system without any grid."
  )

  require(
    grid.forall(row => row.size == grid(0).size),
    "A grid quorum assumes equal sized rows."
  )

  private val gridSeqSet: Seq[Set[T]] = grid.map(_.toSet)
  private val gridSetSet: Set[Set[T]] = gridSeqSet.toSet

  private val rand = new scala.util.Random(seed)

  override def toString(): String = s"Grid(grid=$grid)"

  override val nodes: Set[T] = gridSetSet.flatten

  override def randomReadQuorum(): Set[T] = gridSeqSet(rand.nextInt(grid.size))

  override def randomWriteQuorum(): Set[T] = {
    val i = rand.nextInt(grid(0).size)
    grid.map(row => row(i)).toSet
  }

  override def isReadQuorum(xs: Set[T]): Boolean = {
    require(
      xs.subsetOf(nodes),
      s"Nodes $xs are not a subset of this quorum system's nodes $nodes."
    )
    gridSetSet.exists(row => row.subsetOf(xs))
  }

  override def isWriteQuorum(xs: Set[T]): Boolean = {
    require(
      xs.subsetOf(nodes),
      s"Nodes $xs are not a subset of this quorum system's nodes $nodes."
    )

    gridSetSet.forall(row => row.exists(x => xs.contains(x)))
  }

  override def isSuperSetOfReadQuorum(xs: Set[T]): Boolean =
    gridSetSet.exists(row => row.subsetOf(xs))

  override def isSuperSetOfWriteQuorum(xs: Set[T]): Boolean =
    gridSetSet.forall(row => row.exists(x => xs.contains(x)))
}
