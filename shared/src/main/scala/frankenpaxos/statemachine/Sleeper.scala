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
        // TODO(mwhittaker): Re-implement. Thread.sleep doesn't have small
        // enough granularity.
        if (r.sleepNanos > 0) {
          Thread.sleep(r.sleepNanos / 1000000, (r.sleepNanos % 1000000).toInt)
        }
        SleeperOutput()
      case Request.Empty =>
        throw new IllegalStateException("Empty SleeperInput request.")
    }
  }

  override def typedConflicts(
      firstCommand: SleeperInput,
      secondCommand: SleeperInput
  ): Boolean =
    false

  override def typedConflictIndex[Key](): ConflictIndex[Key, SleeperInput] = {
    new ConflictIndex[Key, SleeperInput] {
      private val inputs = mutable.Map[Key, SleeperInput]()
      override def put(key: Key, command: SleeperInput): Option[SleeperInput] =
        inputs.put(key, command)
      override def get(key: Key): Option[SleeperInput] = inputs.get(key)
      override def remove(key: Key): Option[SleeperInput] = inputs.remove(key)
      override def getConflicts(key: Key, command: SleeperInput): Set[Key] =
        Set()
    }
  }
}
