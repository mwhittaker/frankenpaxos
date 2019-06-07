package frankenpaxos.epaxos

import scala.scalajs.js.annotation._

// By default, Scala case classes cannot be compared using comparators like `<`
// or `>`. This is particularly annoying for Ballots because we often want to
// compare them. The BallotsOrdering implicit class below allows us to do just
// that.
@JSExportAll
object BallotHelpers {
  object Ordering extends scala.math.Ordering[Ballot] {
    private def ballotToTuple(ballot: Ballot): (Int, Int) = {
      val Ballot(ordering, replicaIndex) = ballot
      (ordering, replicaIndex)
    }

    override def compare(lhs: Ballot, rhs: Ballot): Int = {
      val ordering = scala.math.Ordering.Tuple2[Int, Int]
      ordering.compare(ballotToTuple(lhs), ballotToTuple(rhs))
    }
  }

  def inc(ballot: Ballot): Ballot = {
    val Ballot(ordering, replicaIndex) = ballot
    Ballot(ordering + 1, replicaIndex)
  }

  def max(lhs: Ballot, rhs: Ballot): Ballot = {
    Ordering.max(lhs, rhs)
  }

  implicit class BallotImplicits(lhs: Ballot) {
    def <(rhs: Ballot) = Ordering.lt(lhs, rhs)
    def <=(rhs: Ballot) = Ordering.lteq(lhs, rhs)
    def >(rhs: Ballot) = Ordering.gt(lhs, rhs)
    def >=(rhs: Ballot) = Ordering.gteq(lhs, rhs)
  }
}
