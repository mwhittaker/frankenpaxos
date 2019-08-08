package frankenpaxos.simplebpaxos

import frankenpaxos.statemachine.ConflictIndex
import frankenpaxos.statemachine.StateMachine
import scala.collection.mutable
import scala.scalajs.js.annotation._
import scalatags.Text.all._

@JSExportAll
class CompactConflictIndex(numLeaders: Int, stateMachine: StateMachine) {
  private val factory = VertexIdPrefixSet.factory(numLeaders)

  @JSExport
  protected var newConflictIndex = stateMachine.conflictIndex[VertexId]()

  @JSExport
  protected var newWatermark = mutable.Buffer.fill(numLeaders)(0)

  @JSExport
  protected var oldConflictIndex = stateMachine.conflictIndex[VertexId]()

  @JSExport
  protected var oldWatermark = mutable.Buffer.fill(numLeaders)(0)

  @JSExport
  protected var gcWatermark = mutable.Buffer.fill(numLeaders)(0)

  def put(vertexId: VertexId, command: Array[Byte]): Unit = {
    newConflictIndex.put(vertexId, command)
    updateWatermark(newWatermark, vertexId.leaderIndex, vertexId.id + 1)
  }

  def getConflicts(command: Array[Byte]): VertexIdPrefixSet = {
    factory
      .fromSet(newConflictIndex.getConflicts(command))
      .union(factory.fromSet(oldConflictIndex.getConflicts(command)))
      .union(VertexIdPrefixSet(gcWatermark))
  }

  def garbageCollect(): Unit = {
    gcWatermark = pairwiseMaxWatermark(gcWatermark, oldWatermark)
    oldConflictIndex = newConflictIndex
    oldWatermark = newWatermark
    newConflictIndex = stateMachine.conflictIndex[VertexId]()
    newWatermark = mutable.Buffer.fill(numLeaders)(0)
  }

  private def updateWatermark(
      watermark: mutable.Buffer[Int],
      index: Int,
      value: Int
  ): Unit = {
    watermark(index) = Math.max(watermark(index), value)
  }

  private def pairwiseMaxWatermark(
      lhs: mutable.Buffer[Int],
      rhs: mutable.Buffer[Int]
  ): mutable.Buffer[Int] = {
    lhs.zip(rhs).map({ case (l, r) => Math.max(l, r) })
  }
}
