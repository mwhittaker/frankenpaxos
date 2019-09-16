package frankenpaxos.epaxos

import frankenpaxos.compact.CompactSet
import frankenpaxos.compact.IntPrefixSet
import frankenpaxos.util
import frankenpaxos.util.TopK
import frankenpaxos.util.TopOne
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object InstancePrefixSet {
  @JSExport("apply")
  def apply(numReplicas: Int): InstancePrefixSet = {
    new InstancePrefixSet(numReplicas,
                          mutable.Buffer.fill(numReplicas)(IntPrefixSet()))
  }

  def fromWatermarks(watermarks: mutable.Buffer[Int]): InstancePrefixSet = {
    val intPrefixSets = mutable.Buffer[IntPrefixSet]()
    for (watermark <- watermarks) {
      intPrefixSets += IntPrefixSet.fromWatermark(watermark)
    }
    new InstancePrefixSet(watermarks.size, intPrefixSets)
  }

  def fromTopOne(topOne: TopOne[Instance]): InstancePrefixSet =
    InstancePrefixSet.fromWatermarks(topOne.get())

  def fromTopK(topK: TopK[Instance]): InstancePrefixSet = {
    val sortedSets = topK.get()
    val numLeaders = sortedSets.size
    val intPrefixSets = mutable.Buffer[IntPrefixSet]()
    for (sortedSet <- sortedSets) {
      if (sortedSet.size == 0) {
        intPrefixSets += IntPrefixSet()
      } else if (sortedSet.size == 1) {
        intPrefixSets += IntPrefixSet.fromWatermark(sortedSet.head + 1)
      } else {
        intPrefixSets += IntPrefixSet.fromWatermarkAndMutableSet(
          sortedSet.head + 1,
          sortedSet.drop(1).to[mutable.Set]
        )
      }
    }
    new InstancePrefixSet(numLeaders, intPrefixSets)
  }

  def fromProto(proto: InstancePrefixSetProto): InstancePrefixSet = {
    new InstancePrefixSet(
      proto.numReplicas,
      proto.intPrefixSet.map(IntPrefixSet.fromProto).to[mutable.Buffer]
    )
  }
}

@JSExportAll
class InstancePrefixSet private (
    numReplicas: Int,
    val intPrefixSets: mutable.Buffer[IntPrefixSet]
) extends CompactSet[InstancePrefixSet] {
  override type T = Instance

  override def toString(): String = intPrefixSets.toString()

  private def toTuple(): (Int, mutable.Buffer[IntPrefixSet]) =
    (numReplicas, intPrefixSets)

  override def equals(other: Any): Boolean = {
    other match {
      case other: InstancePrefixSet => toTuple() == other.toTuple()
      case _                        => false
    }
  }

  override def hashCode: Int = toTuple().hashCode

  override def add(instance: Instance): Boolean =
    intPrefixSets(instance.replicaIndex).add(instance.instanceNumber)

  override def contains(instance: Instance): Boolean =
    intPrefixSets(instance.replicaIndex).contains(instance.instanceNumber)

  override def union(other: InstancePrefixSet): InstancePrefixSet = {
    new InstancePrefixSet(
      numReplicas,
      for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets))
        yield lhs.union(rhs)
    )
  }

  override def diff(other: InstancePrefixSet): InstancePrefixSet = {
    new InstancePrefixSet(
      numReplicas,
      for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets))
        yield lhs.diff(rhs)
    )
  }

  override def materializedDiff(
      other: InstancePrefixSet
  ): Iterable[Instance] = {
    intPrefixSets.view
      .zip(other.intPrefixSets)
      .zipWithIndex
      .flatMap({
        case ((lhs, rhs), i) => lhs.materializedDiff(rhs).map(Instance(i, _))
      })
  }

  override def diffIterator(other: InstancePrefixSet): Iterator[Instance] = {
    var iterator = Iterator[Instance]()
    for (i <- 0 until numReplicas) {
      iterator ++= intPrefixSets(i)
        .diffIterator(other.intPrefixSets(i))
        .map(Instance(i, _))
    }
    iterator
  }

  override def addAll(other: InstancePrefixSet): this.type = {
    for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets)) {
      lhs.addAll(rhs)
    }
    this
  }

  override def subtractAll(other: InstancePrefixSet): this.type = {
    for ((lhs, rhs) <- intPrefixSets.zip(other.intPrefixSets)) {
      lhs.subtractAll(rhs)
    }
    this
  }

  override def subtractOne(instance: Instance): this.type = {
    intPrefixSets(instance.replicaIndex).subtractOne(instance.instanceNumber)
    this
  }

  override def size: Int = intPrefixSets.map(_.size).sum

  override def uncompactedSize: Int = intPrefixSets.map(_.uncompactedSize).sum

  override def subset(): InstancePrefixSet =
    new InstancePrefixSet(numReplicas, intPrefixSets.map(_.subset()))

  override def materialize(): Set[Instance] = {
    {
      for {
        (intPrefixSet, replicaIndex) <- intPrefixSets.zipWithIndex
        instanceNumber <- intPrefixSet.materialize
      } yield
        Instance(replicaIndex = replicaIndex, instanceNumber = instanceNumber)
    }.toSet
  }

  def toProto(): InstancePrefixSetProto = {
    InstancePrefixSetProto(numReplicas = numReplicas,
                           intPrefixSet = intPrefixSets.map(_.toProto))
  }

  def getWatermark(): Seq[Int] =
    intPrefixSets.map(_.getWatermark)
}
