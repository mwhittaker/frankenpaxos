package frankenpaxos.simplegcbpaxos

import frankenpaxos.statemachine.ConflictIndex
import frankenpaxos.statemachine.StateMachine
import scala.collection.mutable
import scala.scalajs.js.annotation._

// A CompactConflictIndex is a compact, garbage collectable ConflictIndex.
//
// Every dependency service node maintains an array of commands keyed by leader
// index and id that looks something like this:
//
//   3 |   |   |   |
//     +---+---+---+
//   2 |   |   | e |
//     +---+---+---+ i
//   1 |   | b |   | d
//     +---+---+---+
//   0 | a | c | d |
//     +---+---+---+
//       0   1   2
//     leader  index
//
// When a dependency service node receives a command, it first computes the set
// of array entries that contain a command that conflicts with it. These array
// entries are the _dependencies_ of the command.
//
// For example, imagine that a dependency service with the array above received
// command `f`. Assume `f` conflicts with `a`, `b`, `c`, and `d` but not `e`.
// The dependency service node would compute (0, 0), (1, 0), (1, 1), and (2, 0)
// as the dependencies of `f`.
//
// After computing dependencies, a dependency service node then inserts the
// command into the array. Again, if our dependency service received `f`, it
// would update the array to look something like this:
//
//   3 |   |   |   |
//     +---+---+---+
//   2 |   |   | e |
//     +---+---+---+
//   1 |   | b | f |
//     +---+---+---+
//   0 | a | c | d |
//     +---+---+---+
//       0   1   2
//
// Over time, this array grows, so we have to garbage collect it.
// CompactConflictIndex maintains a watermark `gcWatermark` under which all
// commands have been garbage collected. For example, a dependency service
// node may look like this:
//
//   4 |   |   |   |
//     +---+---+---+
//   3 |   |   |   |
//     +---+---+---+
//   2 |   |   |   |
//     +---+---+---+
//   1 |   |###|   |
//     +---+---+---+
//   0 |   |###|###|
//     +---+---+---+
//       0   1   2
//
// Here, the dependency service node has garbage collected all entries under
// the `gcWatermark` [0, 2, 1]. Now, when a dependency service receives a
// command, it computes its dependencies as usual, _but_ it makes sure to
// include all instances that have been garbage collected as well. For example,
// if the dependency service node above received a command `a`, it would
// include (1, 0), (1, 1), and (2, 0) in its dependencies.
//
// All non garbage collected commands are stored in one of two conflict indexes
// (and maybe both): `oldConflictIndex` and `newConflictIndex`. New entries are
// added to the newConflictIndex. Whenever `garbageCollect` is called, all
// instances covered by commands in oldConflictIndex are garbage collected, and
// the newConflictIndex becomes the oldConflictIndex.
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

  def putSnapshot(vertexId: VertexId): Unit = {
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

  // highWatermark returns a watermark that includes all received commands, and
  // maybe more.
  def highWatermark(): VertexIdPrefixSet = {
    val watermark = mutable.Buffer.fill(numLeaders)(0)
    for (i <- 0 until numLeaders) {
      watermark(i) =
        Math.max(Math.max(gcWatermark(i), oldWatermark(i)), newWatermark(i))
    }
    VertexIdPrefixSet(watermark.toSeq)
  }

  private def updateWatermark(
      watermark: mutable.Buffer[Int],
      index: Int,
      value: Int
  ): Unit = {
    watermark(index) = Math.max(watermark(index), value)
  }
}
