package frankenpaxos.statemachine

// A deterministic state machine. A state machine begins in some initial state
// when it is constructed. Then, the state machine can repeatedly execute
// commands using the `run` method. `run` produces an ouput for every input.
//
// TODO(mwhittaker): If we implement speculative execution, the state machine
// abstraction should support undo.
trait StateMachine {
  def run(input: Array[Byte]): Array[Byte]

  // TODO(mwhittaker): Re-think whether this API is the one we want.
  def conflicts(firstCommand: Array[Byte], secondCommand: Array[Byte]): Boolean
}
