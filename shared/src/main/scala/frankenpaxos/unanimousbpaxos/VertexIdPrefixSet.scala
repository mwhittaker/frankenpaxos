package frankenpaxos.unanimousbpaxos

import frankenpaxos.compact.CompactSet
import frankenpaxos.compact.IntPrefixSet
import frankenpaxos.util
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object VertexIdPrefixSet {
  @JSExport("apply")
  def apply(numLeaders: Int): VertexIdPrefixSet = {
    new VertexIdPrefixSet(numLeaders,
                          mutable.Buffer.fill(numLeaders)(IntPrefixSet()))
  }
}

@JSExportAll
class VertexIdPrefixSet private (
    numLeaders: Int,
    val intPrefixSets: mutable.Buffer[IntPrefixSet]
) extends CompactSet[VertexIdPrefixSet] {
  override type T = VertexId

  override def toString(): String = intPrefixSets.toString()

  override def add(vertexId: VertexId): Boolean =
    intPrefixSets(vertexId.leaderIndex).add(vertexId.id)

  override def contains(vertexId: VertexId): Boolean =
    intPrefixSets(vertexId.leaderIndex).contains(vertexId.id)

  override def union(other: VertexIdPrefixSet): VertexIdPrefixSet = {
    new VertexIdPrefixSet(
      numLeaders,
      for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets))
        yield lhs.union(rhs)
    )
  }

  override def diff(other: VertexIdPrefixSet): VertexIdPrefixSet = {
    new VertexIdPrefixSet(
      numLeaders,
      for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets))
        yield lhs.diff(rhs)
    )
  }

  override def addAll(other: VertexIdPrefixSet): this.type = {
    for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets)) {
      lhs.addAll(rhs)
    }
    this
  }

  override def subtractAll(other: VertexIdPrefixSet): this.type = {
    for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets)) {
      lhs.subtractAll(rhs)
    }
    this
  }

  override def size: Int = intPrefixSets.map(_.size).sum

  override def uncompactedSize: Int = intPrefixSets.map(_.uncompactedSize).sum

  override def subset(): VertexIdPrefixSet =
    new VertexIdPrefixSet(numLeaders, intPrefixSets.map(_.subset()))

  override def materialize(): Set[VertexId] = {
    {
      for {
        (intPrefixSet, leaderIndex) <- intPrefixSets.zipWithIndex
        id <- intPrefixSet.materialize
      } yield VertexId(leaderIndex = leaderIndex, id = id)
    }.toSet
  }

  def getWatermark(): Seq[Int] =
    intPrefixSets.map(_.getWatermark)
}
