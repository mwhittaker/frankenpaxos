package frankenpaxos.simplebpaxos

import frankenpaxos.compact.CompactSet
import frankenpaxos.compact.CompactSetFactory
import frankenpaxos.statemachine.ConflictIndex

// DO_NOT_SUBMIT(mwhittaker): Document.
class CompactConflictIndex[
    Key,
    Command,
    KeySet <: CompactSet[KeySet] { type T = Key }
](
    conflictIndex: ConflictIndex[Key, Command]
)(
    implicit keySetFactory: CompactSetFactory[KeySet, Key]
) {
  // The set of keys that have ever been stored in `conflictIndex` or garbage
  // collected from `conflictIndex`.
  private var keys = keySetFactory.empty

  // The set of keys that have been garbage collected from `conflictIndex`.
  private var garbageCollectedWatermark = keySetFactory.empty

  override def toString(): String =
    s"CompactConflictIndex($conflictIndex, $keys, $garbageCollectedWatermark)"

  def put(key: Key, command: Command): Unit = {
    if (garbageCollectedWatermark.contains(key)) {
      // If we've already garbage collected this key, don't add it.
    } else {
      keys.add(key)
      conflictIndex.put(key, command)
    }
  }

  def getConflicts(command: Command): KeySet = {
    keySetFactory
      .fromSet(conflictIndex.getConflicts(command))
      .union(garbageCollectedWatermark)
  }

  def watermark(): KeySet = keys.subset()

  def garbageCollect(garbage: KeySet): Unit = {
    garbageCollectedWatermark = garbageCollectedWatermark.union(garbage)
    keys = keys.union(garbageCollectedWatermark)
    conflictIndex.filterInPlace({
      case (key, _) => !garbageCollectedWatermark.contains(key)
    })
  }
}
