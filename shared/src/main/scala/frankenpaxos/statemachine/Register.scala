package frankenpaxos.statemachine

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class Register extends StateMachine {
  private var x: String = ""

  override def toString(): String = x

  override def run(input: Array[Byte]): Array[Byte] = {
    x = new String(input)
    input
  }

  override def toBytes(): Array[Byte] = x.getBytes()

  override def fromBytes(snapshot: Array[Byte]): Unit = {
    x = new String(snapshot)
  }

  // We say every pair of commands conflict. Technically, if two strings are
  // the same, they don't conflict, but to keep things simple, we say
  // everything conflicts.
  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = true

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] =
    new ConflictIndex[Key, Array[Byte]] {
      private val commandsAndSnapshots = mutable.Set[Key]()

      override def put(key: Key, command: Array[Byte]): Unit =
        commandsAndSnapshots += key

      override def putSnapshot(key: Key): Unit =
        commandsAndSnapshots += key

      override def remove(key: Key): Unit =
        commandsAndSnapshots -= key

      // Since every pair of commands conflict, we return every key.
      override def getConflicts(command: Array[Byte]): Set[Key] =
        commandsAndSnapshots.toSet
    }

  override def topKConflictIndex[Key](
      k: Int,
      numLeaders: Int,
      like: VertexIdLike[Key]
  ): ConflictIndex[Key, Array[Byte]] = {
    if (k == 1) {
      new ConflictIndex[Key, Array[Byte]] {
        private val topOne = new TopOne(numLeaders, like)

        override def put(key: Key, command: Array[Byte]): Unit = topOne.put(key)
        override def putSnapshot(key: Key): Unit = topOne.put(key)
        override def getTopOneConflicts(command: Array[Byte]): TopOne[Key] =
          topOne
      }
    } else {
      new ConflictIndex[Key, Array[Byte]] {
        private val topK = new TopK[Key](k, numLeaders, like)

        override def put(key: Key, command: Array[Byte]): Unit = topK.put(key)
        override def putSnapshot(key: Key): Unit = topK.put(key)
        override def getTopKConflicts(command: Array[Byte]): TopK[Key] = topK
      }
    }
  }
}
