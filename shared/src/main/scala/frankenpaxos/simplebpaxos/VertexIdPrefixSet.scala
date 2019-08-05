package frankenpaxos.simplebpaxos

import frankenpaxos.compact.CompactSet
import frankenpaxos.compact.IntPrefixSet
import frankenpaxos.util
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object VertexIdPrefixSet {
  // Construct an empty VertexIdPrefixSet.
  @JSExport("apply")
  def apply(numLeaders: Int): VertexIdPrefixSet = {
    new VertexIdPrefixSet(numLeaders,
                          mutable.Buffer.fill(numLeaders)(IntPrefixSet()))
  }

  // Construct a VertexIdPrefixSet from a set of uncompacted vertex ids.
  @JSExport("apply")
  def apply(
      numLeaders: Int,
      vertexIds: Set[VertexId]
  ): VertexIdPrefixSet = {
    val idsByLeader = vertexIds.groupBy(_.leaderIndex)
    new VertexIdPrefixSet(
      numLeaders,
      (0 until numLeaders)
        .map(leaderId => idsByLeader.getOrElse(leaderId, Set[VertexId]()))
        .map(vertexIds => vertexIds.map(_.id))
        .map(IntPrefixSet(_))
        .to[mutable.Buffer]
    )
  }

  // Construct a VertexIdPrefixSet from a proto produced by
  // VertexIdPrefixSet.toProto.
  def fromProto(proto: VertexIdPrefixSetProto): VertexIdPrefixSet = {
    new VertexIdPrefixSet(
      proto.numLeaders,
      proto.intPrefixSet.map(IntPrefixSet.fromProto).to[mutable.Buffer]
    )
  }
}

@JSExportAll
class VertexIdPrefixSet private (
    val numLeaders: Int,
    val intPrefixSets: mutable.Buffer[IntPrefixSet]
) extends CompactSet[VertexIdPrefixSet] {
  override type T = VertexId

  private def toTuple(): (Int, mutable.Buffer[IntPrefixSet]) =
    (numLeaders, intPrefixSets)

  override def equals(other: Any): Boolean = {
    other match {
      case other: VertexIdPrefixSet => toTuple() == other.toTuple()
      case _                        => false
    }
  }

  override def hashCode: Int = toTuple().hashCode

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

  def toProto(): VertexIdPrefixSetProto = {
    VertexIdPrefixSetProto(numLeaders = numLeaders,
                           intPrefixSet = intPrefixSets.map(_.toProto))
  }

  def getWatermark(): Seq[Int] =
    intPrefixSets.map(_.getWatermark)
}
