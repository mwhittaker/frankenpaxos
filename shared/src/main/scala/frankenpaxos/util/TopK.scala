package frankenpaxos.util

import collection.mutable
import scala.scalajs.js.annotation._

class TopK[V](k: Int, numLeaders: Int, like: VertexIdLike[V]) {
  type LeaderIndex = Int

  @JSExport
  protected val topOnes = mutable.Buffer
    .fill[mutable.SortedSet[Int]](numLeaders)(mutable.SortedSet[Int]())

  def put(x: V): Unit = {
    val ids = topOnes(like.leaderIndex(x))
    ids += like.id(x)
    if (ids.size > k) {
      ids.remove(ids.head)
    }
  }

  def get(): mutable.Buffer[mutable.SortedSet[Int]] = topOnes

  def mergeEquals(other: TopK[V]): Unit = {
    for (i <- 0 until numLeaders) {
      val ids = topOnes(i)
      ids ++= other.topOnes(i)
      while (ids.size > k) {
        ids.remove(ids.head)
      }
    }
  }
}
