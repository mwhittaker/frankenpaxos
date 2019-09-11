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

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] = {
    new ConflictIndex[Key, Array[Byte]] {
      private val commands = mutable.Map[Key, Array[Byte]]()

      override def put(key: Key, command: Array[Byte]): Option[Array[Byte]] =
        commands.put(key, command)

      override def remove(key: Key): Option[Array[Byte]] =
        commands.remove(key)

      // Since every pair of commands conflict, we return every key except for
      // `key`.
      override def getConflicts(command: Array[Byte]): Set[Key] =
        commands.keys.toSet
    }
  }

  override def topKConflictIndex[Key](
      k: Int,
      like: VertexIdLike[Key]
  ): ConflictIndex[Key, Array[Byte]] = {
    if (k == 1) {
      new ConflictIndex[Key, Array[Byte]] {
        private val topOne = new TopOne(like)

        override def put(
            key: Key,
            command: Array[Byte]
        ): Option[Array[Byte]] = {
          topOne.put(key)
          None
        }

        override def remove(key: Key): Option[Array[Byte]] = ???

        override def getConflicts(command: Array[Byte]): Set[Key] =
          topOne.get()
      }
    } else {
      new ConflictIndex[Key, Array[Byte]] {
        private val topK = new TopK[Key](k, like)

        override def put(
            key: Key,
            command: Array[Byte]
        ): Option[Array[Byte]] = {
          topK.put(key)
          None
        }

        override def remove(key: Key): Option[Array[Byte]] = ???

        override def getConflicts(command: Array[Byte]): Set[Key] =
          topK.get()
      }
    }
  }
}
