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

      override def get(key: Key): Option[Array[Byte]] =
        commands.get(key)

      override def remove(key: Key): Option[Array[Byte]] =
        commands.remove(key)

      // Since every pair of commands conflict, we return every key except for
      // `key`.
      override def getConflicts(key: Key, command: Array[Byte]): Set[Key] =
        commands.filterKeys(_ != key).keys.to[Set]
    }
  }
}
