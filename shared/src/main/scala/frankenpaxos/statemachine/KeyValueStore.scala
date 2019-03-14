package frankenpaxos.statemachine

import frankenpaxos.Logger

import collection.mutable
import scala.scalajs.js.annotation._

object InputSerializer extends frankenpaxos.ProtoSerializer[Input]

object OutputSerializer extends frankenpaxos.ProtoSerializer[Output]

@JSExportAll
class KeyValueStore extends TypedStateMachine[Input, Output] {
  private val kvs = mutable.Map[String, String]()
  private var state = ""
  var debug = ""

  override def toString(): String =
    kvs.toString()

  override val inputSerializer = InputSerializer
  override val outputSerializer = OutputSerializer

  override def typedRun(input: Input): Output = {
    import Input.Request
    input.request match {
      case Request.GetRequest(GetRequest(keys)) =>
        Output().withGetReply(
          GetReply(keys.map(k => GetKeyValuePair(k, kvs.get(k))))
        )

      case Request.SetRequest(SetRequest(keyValues)) =>
        keyValues.foreach({ case SetKeyValuePair(k, v) => kvs(k) = v })
        Output().withSetReply(SetReply())

      case Request.Empty =>
        throw new IllegalStateException()
    }
  }

  override def typedConflicts(firstCommand: Input, secondCommand: Input): Boolean = {
    import Input.Request
    var firstValue: Seq[String] = null
    var secondValue: Seq[String] = null

    debug = "Commands 1: " + firstCommand.request.toString + "\n"
    debug = debug + "Commands 2: "  + secondCommand.request.toString
    if (firstCommand.request.isGetRequest && secondCommand.request.isGetRequest)
      return false
    firstCommand.request match {
      case Request.SetRequest(SetRequest(keyValues)) => firstValue = keyValues.map(_.key)
      case Request.GetRequest(GetRequest(keys)) => firstValue = keys
    }
    secondCommand.request match {
      case Request.SetRequest(SetRequest(keyValues)) => secondValue = keyValues.map(_.key)
      case Request.GetRequest(GetRequest(keys)) => secondValue = keys
    }
    firstValue.intersect(secondValue).nonEmpty
  }
}
