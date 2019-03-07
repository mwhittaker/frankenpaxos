package frankenpaxos.statemachine

// A StateMachine takes in strings and outputs strings. A TypedStateMachine, on
// the other hand, is parameterized by an input type I and output type O.
// TypedStateMachines are useful when inputs and outputs are non-trivial (e.g.,
// when they are protobufs).
trait TypedStateMachine[I, O] extends StateMachine {
  def inputSerializer: frankenpaxos.Serializer[I]
  def outputSerializer: frankenpaxos.Serializer[O]
  def typedRun(input: I): O

  override def run(input: Array[Byte]): Array[Byte] = {
    val output = typedRun(inputSerializer.fromBytes(input))
    outputSerializer.toBytes(output)
  }
}
