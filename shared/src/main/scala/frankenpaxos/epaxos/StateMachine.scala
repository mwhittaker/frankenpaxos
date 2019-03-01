package frankenpaxos.epaxos
import scala.collection.mutable.ListBuffer

class StateMachine {

  val state: ListBuffer[String] = ListBuffer()

  def executeCommand(command: String): Unit = {
    state.append(command)
  }

  def getCurrentState(): String = {
    state.toString()
  }

  override def toString: String = {
    state.toString()
  }
}
