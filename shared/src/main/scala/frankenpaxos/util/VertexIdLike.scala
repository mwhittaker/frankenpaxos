package frankenpaxos.util

// BPaxos VertexIds and EPaxos Instances are really the same thing. They
// consist of a leader index and a monotonically increasing id. A VertexIdLike
// object abstracts over the two. If you want a method to be able to handle
// VertexIds or Instances, you'd write it like this:
//
//   def foo[T](x: T, vid: VertexIdLike[T])
trait VertexIdLike[T] {
  def leaderIndex(x: T): Int
  def id(x: T): Int
  def make(leaderIndex: Int, id: Int): T

  val intraLeaderOrdering = new scala.math.Ordering[T] {
    override def compare(x: T, y: T): Int =
      scala.math.Ordering.Int.compare(id(x), id(y))
  }
}
