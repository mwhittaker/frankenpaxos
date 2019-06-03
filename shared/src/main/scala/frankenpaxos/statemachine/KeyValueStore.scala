package frankenpaxos.statemachine

import collection.mutable
import scala.scalajs.js.annotation._

object KeyValueStoreInputSerializer
    extends frankenpaxos.ProtoSerializer[KeyValueStoreInput]

object KeyValueStoreOutputSerializer
    extends frankenpaxos.ProtoSerializer[KeyValueStoreOutput]

@JSExportAll
class KeyValueStore
    extends TypedStateMachine[KeyValueStoreInput, KeyValueStoreOutput] {
  private val kvs = mutable.Map[String, String]()

  // TODO(mwhittaker): Remove.
  var executedCommands: mutable.ListBuffer[KeyValueStoreInput] =
    mutable.ListBuffer[KeyValueStoreInput]()

  override def toString(): String = kvs.toString()

  override val inputSerializer = KeyValueStoreInputSerializer
  override val outputSerializer = KeyValueStoreOutputSerializer

  override def typedRun(input: KeyValueStoreInput): KeyValueStoreOutput = {
    // TODO(mwhittaker): Remove.
    executedCommands.append(input)

    import KeyValueStoreInput.Request
    input.request match {
      case Request.GetRequest(GetRequest(keys)) =>
        KeyValueStoreOutput().withGetReply(
          GetReply(keys.map(k => GetKeyValuePair(k, kvs.get(k))))
        )

      case Request.SetRequest(SetRequest(keyValues)) =>
        keyValues.foreach({ case SetKeyValuePair(k, v) => kvs(k) = v })
        KeyValueStoreOutput().withSetReply(SetReply())

      case Request.Empty =>
        throw new IllegalStateException()
    }
  }

  private def keys(input: KeyValueStoreInput): Set[String] = {
    import KeyValueStoreInput.Request
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
      firstCommand: KeyValueStoreInput,
      secondCommand: KeyValueStoreInput
  ): Boolean = {
    import KeyValueStoreInput.Request

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
