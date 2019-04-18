package frankenpaxos.statemachine

import scala.scalajs.js.annotation._

@JSExportAll
class Register extends StateMachine {
  private var x: String = ""

  override def toString(): String = x

  override def run(input: Array[Byte]): Array[Byte] = {
    x = new String(input)
    input
  }

  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = true
}
