package frankenpaxos.simplebpaxos

import frankenpaxos.util.BufferMap
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class VertexIdBufferMap[V](val numLeaders: Int, val growSize: Int = 5000) {
  @JSExport
  protected val bufferMaps: mutable.Buffer[BufferMap[V]] =
    mutable.Buffer.fill(numLeaders)(new BufferMap(growSize))

  def get(vertexId: VertexId): Option[V] =
    bufferMaps(vertexId.leaderIndex).get(vertexId.id)

  def put(vertexId: VertexId, value: V): Unit =
    bufferMaps(vertexId.leaderIndex).put(vertexId.id, value)

  def garbageCollect(watermark: Seq[Int]): Unit = {
    require(watermark.size == numLeaders)
    bufferMaps
      .zip(watermark)
      .foreach({ case (map, watermark) => map.garbageCollect(watermark) })
  }
}
