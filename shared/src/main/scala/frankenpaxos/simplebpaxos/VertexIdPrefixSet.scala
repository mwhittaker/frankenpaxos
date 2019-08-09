package frankenpaxos.simplebpaxos

import frankenpaxos.compact.CompactSet
import frankenpaxos.compact.CompactSetFactory
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
    if (vertexIds.isEmpty) {
      return new VertexIdPrefixSet(
        numLeaders,
        mutable.Buffer.fill(numLeaders)(IntPrefixSet())
      )
    }

    val sets = mutable.Buffer.fill(numLeaders)(mutable.Set[Int]())
    for (VertexId(leaderIndex, id) <- vertexIds) {
      sets(leaderIndex).add(id)
    }
    new VertexIdPrefixSet(
      numLeaders,
      sets.map(IntPrefixSet.fromMutableSet(_))
    )
  }

  // Construct a VertexIdPrefixSet from a set of watermarks.
  @JSExport("apply")
  def apply(watermarks: Seq[Int]): VertexIdPrefixSet = {
    val intPrefixSets = mutable.Buffer[IntPrefixSet]()
    for (watermark <- watermarks) {
      intPrefixSets += IntPrefixSet.fromWatermark(watermark)
    }
    new VertexIdPrefixSet(watermarks.size, intPrefixSets)
  }

  // Construct a VertexIdPrefixSet from a set of watermarks and a set of
  // VertexIds. It is assumed but not enforced that every vertex id in
  // VertexIds is larger than the corresponding watermark.
  def fromWatermarksAndSet(
      watermarks: Seq[Int],
      vertexIds: Set[VertexId]
  ): VertexIdPrefixSet = {
    val numLeaders = watermarks.size
    val sets = mutable.Buffer.fill(numLeaders)(mutable.Set[Int]())
    for (VertexId(leaderIndex, id) <- vertexIds) {
      sets(leaderIndex).add(id)
    }

    val intPrefixSets = mutable.Buffer[IntPrefixSet]()
    for ((watermark, set) <- watermarks.zip(sets)) {
      intPrefixSets += IntPrefixSet.fromWatermarkAndMutableSet(watermark, set)
    }
    new VertexIdPrefixSet(watermarks.size, intPrefixSets)
  }

  // Construct a VertexIdPrefixSet from a proto produced by
  // VertexIdPrefixSet.toProto.
  def fromProto(proto: VertexIdPrefixSetProto): VertexIdPrefixSet = {
    new VertexIdPrefixSet(
      proto.numLeaders,
      proto.intPrefixSet.map(IntPrefixSet.fromProto).to[mutable.Buffer]
    )
  }

  def factory(numLeaders: Int) =
    new CompactSetFactory[VertexIdPrefixSet, VertexId] {
      override def empty = VertexIdPrefixSet(numLeaders)
      override def fromSet(xs: Set[VertexId]) =
        VertexIdPrefixSet(numLeaders, xs)
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

  override def toString(): String =
    s"VertexIdPrefixSet(${intPrefixSets.mkString(", ")})"

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

  override def subtractOne(vertexId: VertexId): this.type = {
    intPrefixSets(vertexId.leaderIndex).subtractOne(vertexId.id)
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

  def toProto(): VertexIdPrefixSetProto = {
    VertexIdPrefixSetProto(numLeaders = numLeaders,
                           intPrefixSet = intPrefixSets.map(_.toProto))
  }

  def getWatermark(): Seq[Int] =
    intPrefixSets.map(_.getWatermark)
}
