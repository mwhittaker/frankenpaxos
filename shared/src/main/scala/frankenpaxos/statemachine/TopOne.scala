package frankenpaxos.statemachine

import scala.collection.mutable
import scala.scalajs.js.annotation._

class TopOne[T](like: VertexIdLike[T]) {
  type LeaderIndex = Int

  @JSExport
  protected val topOnes = mutable.Map[LeaderIndex, T]()

  def put(x: T): Unit = {
    topOnes.get(like.leaderIndex(x)) match {
      case Some(y) =>
        if (like.id(x) > like.id(y)) {
          topOnes(like.leaderIndex(x)) = x
        }
      case None =>
        topOnes(like.leaderIndex(x)) = x
    }
  }

  def get(): Set[T] = topOnes.values.toSet

  def mergeEquals(other: TopOne[T]): Unit = {
    for ((leaderIndex, y) <- other.topOnes) {
      topOnes.get(leaderIndex) match {
        case None => topOnes(leaderIndex) = y
        case Some(x) =>
          if (like.id(y) > like.id(x)) {
            topOnes(leaderIndex) = y
          }
      }
    }
  }
}
