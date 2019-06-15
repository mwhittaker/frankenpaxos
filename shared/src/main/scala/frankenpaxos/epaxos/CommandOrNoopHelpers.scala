package frankenpaxos.epaxos

import scala.scalajs.js.annotation._

@JSExportAll
object CommandOrNoopHelpers {
  implicit class CommandOrNoopImplicits(commandOrNoop: CommandOrNoop) {
    def bytes(): Option[Array[Byte]] = {
      commandOrNoop.value match {
        case CommandOrNoop.Value.Command(command) =>
          Some(command.command.toByteArray)
        case CommandOrNoop.Value.Noop(Noop()) =>
          None
        case CommandOrNoop.Value.Empty =>
          None
      }
    }
  }
}
