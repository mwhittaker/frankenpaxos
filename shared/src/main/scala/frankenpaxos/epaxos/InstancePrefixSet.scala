package frankenpaxos.epaxos

import frankenpaxos.util
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object InstancePrefixSet {
  @JSExport("apply")
  def apply(numReplicas: Int): InstancePrefixSet = {
    new InstancePrefixSet(numReplicas,
                          mutable.Buffer.fill(numReplicas)(util.IntPrefixSet()))
  }
}

@JSExportAll
class InstancePrefixSet private (
    numReplicas: Int,
    val intPrefixSets: mutable.Buffer[util.IntPrefixSet]
) extends util.CompactSet[InstancePrefixSet] {
  override type T = Instance

  override def toString(): String = intPrefixSets.toString()

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

  override def size: Int = intPrefixSets.map(_.size).sum

  override def uncompactedSize: Int = intPrefixSets.map(_.uncompactedSize).sum

  override def materialize(): Set[Instance] = {
    {
      for {
        (intPrefixSet, replicaIndex) <- intPrefixSets.zipWithIndex
        instanceNumber <- intPrefixSet.materialize
      } yield
        Instance(replicaIndex = replicaIndex, instanceNumber = instanceNumber)
    }.toSet
  }

  def getWatermark(): Seq[Int] =
    intPrefixSets.map(_.getWatermark)
}
