package frankenpaxos.statemachine

import collection.mutable
import scala.scalajs.js.annotation._

object InputSerializer extends frankenpaxos.ProtoSerializer[Input]

object OutputSerializer extends frankenpaxos.ProtoSerializer[Output]

@JSExportAll
class KeyValueStore extends TypedStateMachine[Input, Output] {
  private val kvs = mutable.Map[String, String]()

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
}
