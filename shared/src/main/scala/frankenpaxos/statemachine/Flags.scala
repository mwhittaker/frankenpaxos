package frankenpaxos.statemachine

// This file contains utilities to specify state machines from the command
// line using scopt. See [1] for more information.
//
// [1]: https://github.com/scopt/scopt

// The types of all known state machines.
sealed trait StateMachineType
case object TRegister extends StateMachineType
case object TAppendLog extends StateMachineType
case object TKeyValueStore extends StateMachineType

object Flags {
  // This implicit value allows us to write scopt code like this:
  //
  //     opt[StateMachineType]('s', "state_machine")
  //       .valueName(valueName)
  //       .action((x, f) => f.copy(stateMachineType = x))
  //       .text("State machine type")
  //
  // See [1] and [2] for more information.
  //
  // [1]: https://github.com/scopt/scopt
  // [2]: http://scopt.github.io/scopt/3.5.0/api/index.html#scopt.Read
  implicit val stateMachineTypeRead: scopt.Read[StateMachineType] =
    scopt.Read.reads({
      case "Register"      => TRegister
      case "AppendLog"     => TAppendLog
      case "KeyValueStore" => TKeyValueStore
      case s =>
        throw new IllegalArgumentException(
          s"$s is not one of Register, AppendLog, or KeyValueStore."
        )
    })

  // `valueName` can be passed to scopt's valueName method. See above for an
  // example.
  val valueName: String = "<Register | AppendLog | KeyValueStore>"

  def make(t: StateMachineType): StateMachine = {
    t match {
      case TRegister      => new Register()
      case TAppendLog     => new AppendLog()
      case TKeyValueStore => new KeyValueStore()
    }
  }
}
