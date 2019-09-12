package frankenpaxos.statemachine

import scala.collection.mutable
import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
class Noop extends StateMachine {
  override def toString(): String = "Noop"

  override def run(input: Array[Byte]): Array[Byte] = Array[Byte]()

  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = false

  override def toBytes(): Array[Byte] = Array[Byte]()

  override def fromBytes(snapshot: Array[Byte]): Unit = {
    // Do nothing.
  }

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] = {
    new ConflictIndex[Key, Array[Byte]] {
      private val noops = mutable.Set[Key]()
      private val snapshots = mutable.Set[Key]()

      override def put(key: Key, command: Array[Byte]): Unit = noops += key
      override def putSnapshot(key: Key): Unit = snapshots.add(key)
      override def remove(key: Key): Unit = {
        noops -= key
        snapshots -= key
      }
      override def getConflicts(command: Array[Byte]): Set[Key] =
        snapshots.toSet
    }
  }

  override def topKConflictIndex[Key](
      k: Int,
      numLeaders: Int,
      like: VertexIdLike[Key]
  ): ConflictIndex[Key, Array[Byte]] =
    if (k == 1) {
      new ConflictIndex[Key, Array[Byte]] {
        private val snapshots = new TopOne(numLeaders, like)

        override def put(key: Key, command: Array[Byte]): Unit = ()
        override def putSnapshot(key: Key): Unit = snapshots.put(key)
        override def getTopOneConflicts(command: Array[Byte]): TopOne[Key] =
          snapshots
      }
    } else {
      new ConflictIndex[Key, Array[Byte]] {
        private val snapshots = new TopK(k, numLeaders, like)

        override def put(key: Key, command: Array[Byte]): Unit = ()
        override def putSnapshot(key: Key): Unit = snapshots.put(key)
        override def getTopKConflicts(command: Array[Byte]): TopK[Key] =
          snapshots
      }
    }
}
