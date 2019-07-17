package frankenpaxos.statemachine

// A deterministic state machine. A state machine begins in some initial state
// when it is constructed. Then, the state machine can repeatedly execute
// commands using the `run` method. `run` produces an ouput for every input.
//
// TODO(mwhittaker): If we implement speculative execution, the state machine
// abstraction should support undo.
trait StateMachine {
  // Abstract value members.
  def run(input: Array[Byte]): Array[Byte]
  def conflicts(firstCommand: Array[Byte], secondCommand: Array[Byte]): Boolean

  // Concrete value members.
  def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] =
    new NaiveConflictIndex(conflicts)
}

object StateMachine {
  implicit val read: scopt.Read[StateMachine] = scopt.Read.reads({
    case "AppendLog"     => new AppendLog()
    case "KeyValueStore" => new KeyValueStore()
    case "Noop"          => new Noop()
    case "Register"      => new Register()
    case x =>
      throw new IllegalArgumentException(
        s"$x is not one of AppendLog, KeyValueStore, Noop, or Register."
      )
  })
}
