package frankenpaxos.unanimousbpaxos

object VertexIdHelpers {
  implicit val vertexIdOrdering = new Ordering[VertexId] {
    override def compare(x: VertexId, y: VertexId): Int = {
      val VertexId(xleaderIndex, xid) = x
      val VertexId(yleaderIndex, yid) = y
      val ordering = Ordering.Tuple2[Int, Int]
      ordering.compare((xleaderIndex, xid), (yleaderIndex, yid))
    }
  }
}
