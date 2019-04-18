package frankenpaxos.epaxos
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.scalajs.js.annotation.{JSExport, JSExportAll}

@JSExportAll
class StateMachine {

  val state: ListBuffer[String] = ListBuffer()

  def init(): mutable.Map[(Array[Byte], Array[Byte]), Boolean] = {
    val data = mutable.Map[(Array[Byte], Array[Byte]), Boolean]()
    data.put((Array(1.toByte), Array(2.toByte)), true)
    data.put((Array(2.toByte), Array(1.toByte)), true)
    data
  }

  val interferenceData: mutable.Map[(Array[Byte], Array[Byte]), Boolean] =
    init()

  def conflicts(commandOne: Array[Byte], commandTwo: Array[Byte]): Boolean = {
    if (ByteString
          .copyFrom(commandOne)
          .equals(ByteString.copyFromUtf8("Noop")) ||
        ByteString
          .copyFrom(commandTwo)
          .equals(ByteString.copyFromUtf8("Noop"))) {
      return false
    }
    interferenceData.getOrElse((commandOne, commandTwo), false)
  }

  def addConflict(commandOne: Array[Byte], commandTwo: Array[Byte]): Unit = {
    interferenceData.put((commandOne, commandTwo), true)
    interferenceData.put((commandTwo, commandOne), true)
  }

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
