package frankenpaxos.statemachine

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class AppendLog extends StateMachine {
  private var xs = mutable.Buffer[String]()

  override def toString(): String = xs.toString()

  override def run(input: Array[Byte]): Array[Byte] = {
    xs += new String(input)
    (xs.size - 1).toString().getBytes()
  }

  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = true

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] =
    new ConflictIndex[Key, Array[Byte]] {
      private val xs = mutable.Map[Key, Array[Byte]]()
      override def toString(): String = xs.toString()
      override def put(key: Key, command: Array[Byte]): Option[Array[Byte]] =
        xs.put(key, command)
      override def remove(key: Key): Option[Array[Byte]] = xs.remove(key)
      override def getConflicts(command: Array[Byte]): Set[Key] = xs.keys.toSet
    }
}
