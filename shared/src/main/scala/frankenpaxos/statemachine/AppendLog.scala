package frankenpaxos.statemachine

import collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class AppendLog extends StateMachine {
  private var xs = mutable.Buffer[String]()

  override def toString(): String = xs.toString()

  override def run(input: Array[Byte]): Array[Byte] = {
    xs += new String(input)
    (xs.size - 1).toString().getBytes()
  }

  override def conflicts(firstCommand: Array[Byte],
                         secondCommand: Array[Byte]): Boolean = true
}
