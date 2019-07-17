package frankenpaxos.epaxos

object InstanceHelpers {
  implicit val instanceOrdering = new Ordering[Instance] {
    override def compare(x: Instance, y: Instance): Int = {
      val Instance(xLeaderIndex, xInstanceNumber) = x
      val Instance(yLeaderIndex, yInstanceNumber) = y
      val ordering = Ordering.Tuple2[Int, Int]
      ordering.compare((xLeaderIndex, xInstanceNumber),
                       (yLeaderIndex, yInstanceNumber))
    }
  }
}
