package frankenpaxos.statemachine

import collection.mutable
import scala.scalajs.js.annotation._

class TopK[T](k: Int, like: VertexIdLike[T]) {
  type LeaderIndex = Int

  @JSExport
  protected val topOnes = mutable.Map[LeaderIndex, mutable.SortedSet[T]]()

  def put(x: T): Unit = {
    topOnes.get(like.leaderIndex(x)) match {
      case Some(ys) =>
        ys += x
        if (ys.size > k) {
          ys.remove(ys.head)
        }
      case None =>
        topOnes(like.leaderIndex(x)) =
          mutable.SortedSet(x)(like.intraLeaderOrdering)
    }
  }

  def get(): Set[T] = topOnes.values.flatten.toSet

  def mergeEquals(other: TopK[T]): Unit = {
    for ((leaderIndex, ys) <- other.topOnes) {
      topOnes.get(leaderIndex) match {
        case None =>
          val xs = mutable.SortedSet()(like.intraLeaderOrdering)
          topOnes(leaderIndex) = xs
          xs ++= ys
        case Some(xs) =>
          xs ++= ys
          while (xs.size > k) {
            xs.remove(xs.head)
          }
      }
    }
  }
}
