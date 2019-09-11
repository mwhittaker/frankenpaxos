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
      private val commands = mutable.Map[Key, Array[Byte]]()
      override def put(key: Key, command: Array[Byte]): Option[Array[Byte]] =
        commands.put(key, command)
      override def remove(key: Key): Option[Array[Byte]] = commands.remove(key)
      override def getConflicts(command: Array[Byte]): Set[Key] = Set()
    }
  }

  override def topKConflictIndex[Key](
      k: Int,
      like: VertexIdLike[Key]
  ): ConflictIndex[Key, Array[Byte]] =
    new ConflictIndex[Key, Array[Byte]] {
      override def put(key: Key, command: Array[Byte]): Option[Array[Byte]] =
        None
      override def remove(key: Key): Option[Array[Byte]] = ???
      override def getConflicts(command: Array[Byte]): Set[Key] = Set()
    }
}
