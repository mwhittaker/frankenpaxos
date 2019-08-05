package frankenpaxos.statemachine

// A StateMachine takes in strings and outputs strings. A TypedStateMachine, on
// the other hand, is parameterized by an input type I and output type O.
// TypedStateMachines are useful when inputs and outputs are non-trivial (e.g.,
// when they are protobufs).
trait TypedStateMachine[I, O] extends StateMachine {
  // Abstract value members.
  def inputSerializer: frankenpaxos.Serializer[I]
  def outputSerializer: frankenpaxos.Serializer[O]
  def typedRun(input: I): O
  def typedConflicts(firstCommand: I, secondCommand: I): Boolean

  // Concrete value members.
  override def run(input: Array[Byte]): Array[Byte] = {
    val output = typedRun(inputSerializer.fromBytes(input))
    outputSerializer.toBytes(output)
  }

  // TODO(mwhittaker): Re-think whether this API is the one we want.
  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = {
    typedConflicts(inputSerializer.fromBytes(firstCommand),
                   inputSerializer.fromBytes(secondCommand))
  }

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] = {
    new ConflictIndex[Key, Array[Byte]] {
      private val index = typedConflictIndex[Key]()

      override def put(key: Key, command: Array[Byte]): Option[Array[Byte]] =
        index
          .put(key, inputSerializer.fromBytes(command))
          .map(inputSerializer.toBytes)

      override def get(key: Key): Option[Array[Byte]] =
        index
          .get(key)
          .map(inputSerializer.toBytes)

      override def remove(key: Key): Option[Array[Byte]] =
        index
          .remove(key)
          .map(inputSerializer.toBytes)

      override def filterInPlace(
          f: (Key, Array[Byte]) => Boolean
      ): this.type = {
        index
          .filterInPlace(
            (key, command) => f(key, inputSerializer.toBytes(command))
          )
        this
      }

      override def getConflicts(command: Array[Byte]): Set[Key] = {
        index.getConflicts(inputSerializer.fromBytes(command))
      }
    }
  }

  def typedConflictIndex[Key](): ConflictIndex[Key, I] =
    new NaiveConflictIndex(typedConflicts)
}
