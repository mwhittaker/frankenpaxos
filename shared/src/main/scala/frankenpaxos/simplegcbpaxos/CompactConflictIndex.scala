package frankenpaxos.simplegcbpaxos

import frankenpaxos.statemachine.ConflictIndex
import frankenpaxos.statemachine.StateMachine
import scala.collection.mutable
import scala.scalajs.js.annotation._

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
  protected val gcWatermark = mutable.Buffer.fill(numLeaders)(0)

  def put(vertexId: VertexId, command: Array[Byte]): Unit = {
    newConflictIndex.put(vertexId, command)
    updateWatermark(newWatermark, vertexId.leaderIndex, vertexId.id + 1)
  }

  def getConflicts(command: Array[Byte]): VertexIdPrefixSet = {
    factory
      .fromSet(
        newConflictIndex.getConflicts(command) ++
          oldConflictIndex.getConflicts(command)
      )
      .addAll(VertexIdPrefixSet(gcWatermark))
  }

  def garbageCollect(): Unit = {
    for (i <- 0 until numLeaders) {
      updateWatermark(gcWatermark, i, oldWatermark(i))
    }
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
}
