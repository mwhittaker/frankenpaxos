package frankenpaxos.epaxos

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
}
