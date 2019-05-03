package frankenpaxos.statemachine

import collection.mutable
import frankenpaxos.Logger
import scala.scalajs.js.annotation._

object SleeperInputSerializer extends frankenpaxos.ProtoSerializer[SleeperInput]

object SleeperOutputSerializer
    extends frankenpaxos.ProtoSerializer[SleeperOutput]

@JSExportAll
class Sleeper extends TypedStateMachine[SleeperInput, SleeperOutput] {
  override val inputSerializer = SleeperInputSerializer
  override val outputSerializer = SleeperOutputSerializer

  override def typedRun(input: SleeperInput): SleeperOutput = {
    import SleeperInput.Request
    input.request match {
      case Request.SleepRequest(r) =>
        Thread.sleep(r.sleepMs)
        SleeperOutput()
      case Request.Empty =>
        throw new IllegalStateException("Empty SleeperInput request.")
    }
  }

  override def typedConflicts(
      firstCommand: SleeperInput,
      secondCommand: SleeperInput
  ): Boolean = {
    true
  }
}
