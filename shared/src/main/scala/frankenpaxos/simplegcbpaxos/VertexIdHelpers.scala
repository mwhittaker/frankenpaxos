package frankenpaxos.simplegcbpaxos

import frankenpaxos.util.VertexIdLike

object VertexIdHelpers {
  implicit val vertexIdOrdering = new Ordering[VertexId] {
    override def compare(x: VertexId, y: VertexId): Int = {
      val VertexId(xleaderIndex, xid) = x
      val VertexId(yleaderIndex, yid) = y
      val ordering = Ordering.Tuple2[Int, Int]
      ordering.compare((xleaderIndex, xid), (yleaderIndex, yid))
    }
  }

  implicit val like = new VertexIdLike[VertexId] {
    override def leaderIndex(vertexId: VertexId) = vertexId.leaderIndex
    override def id(vertexId: VertexId) = vertexId.id
    override def make(leaderIndex: Int, id: Int): VertexId =
      VertexId(leaderIndex, id)
  }
}
