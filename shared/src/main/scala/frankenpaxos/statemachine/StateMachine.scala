package frankenpaxos.statemachine

// A deterministic state machine. A state machine begins in some initial state
// when it is constructed. Then, the state machine can repeatedly execute
// commands using the `run` method. `run` produces an ouput for every input.
//
// TODO(mwhittaker): If we implement speculative execution, the state machine
// abstraction should support undo.
trait StateMachine {
  // `run(input)` executes a state machine command. The state machine
  // transitions to a new state and outputs an output.
  def run(input: Array[Byte]): Array[Byte]

  // `conflicts(x, y)` returns whether commands x and y conflict. Two commands
  // are said to conflict if there exists some state in which executing x and
  // then y behaves differently than executing y and then x, either producing
  // different outputs or leading to different final states.
  def conflicts(firstCommand: Array[Byte], secondCommand: Array[Byte]): Boolean

  // `toBytes` returns a snapshot of the state machine that can be read in
  // using `fromBytes`. toBytes does not change the state of the state machine.
  def toBytes(): Array[Byte]

  // `fromBytes(snapshot)` reads in a snapshot produced by `toBytes`. The state
  // of the state machine is replaced with the state of the snapshot.
  def fromBytes(snapshot: Array[Byte]): Unit

  // Returns a conflict index. The default implementation is inefficient. If
  // you care about efficiency, you should override this method.
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
