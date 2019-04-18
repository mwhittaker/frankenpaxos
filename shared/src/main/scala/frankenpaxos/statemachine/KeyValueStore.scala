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
  var executedCommands: mutable.ListBuffer[Input] = mutable.ListBuffer[Input]()

  override def toString(): String = kvs.toString()

  override val inputSerializer = InputSerializer
  override val outputSerializer = OutputSerializer

  override def typedRun(input: Input): Output = {
    state = state + input.request.toString + "\n"
    executedCommands.append(input)
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

  private def keys(input: Input): Set[String] = {
    import Input.Request
    input.request match {
      case Request.GetRequest(GetRequest(keys)) =>
        keys.to[Set]
      case Request.SetRequest(SetRequest(keyValues)) =>
        keyValues.map({ case SetKeyValuePair(k, _) => k }).to[Set]
      case Request.Empty =>
        throw new IllegalStateException()
    }
  }

  override def typedConflicts(
      firstCommand: Input,
      secondCommand: Input
  ): Boolean = {
    import Input.Request

    (firstCommand.request, secondCommand.request) match {
      case (Request.GetRequest(_), Request.GetRequest(_)) =>
        // Get requests do not conflict.
        false

      case (Request.SetRequest(_), Request.GetRequest(_)) |
          (Request.GetRequest(_), Request.SetRequest(_)) |
          (Request.SetRequest(_), Request.SetRequest(_)) =>
        keys(firstCommand).intersect(keys(secondCommand)).nonEmpty

      case (Request.Empty, _) | (_, Request.Empty) =>
        throw new IllegalStateException()
    }
  }
}
