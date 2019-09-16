package frankenpaxos.epaxos

import frankenpaxos.util.VertexIdLike

object InstanceHelpers {
  implicit val instanceOrdering = new Ordering[Instance] {
    override def compare(x: Instance, y: Instance): Int = {
      val Instance(xReplicaIndex, xInstanceNumber) = x
      val Instance(yReplicaIndex, yInstanceNumber) = y
      val ordering = Ordering.Tuple2[Int, Int]
      ordering.compare((xReplicaIndex, xInstanceNumber),
                       (yReplicaIndex, yInstanceNumber))
    }
  }

  implicit val like = new VertexIdLike[Instance] {
    override def leaderIndex(vertexId: Instance) = vertexId.replicaIndex
    override def id(vertexId: Instance) = vertexId.instanceNumber
    override def make(leaderIndex: Int, id: Int): Instance =
      Instance(leaderIndex, id)
  }
}
