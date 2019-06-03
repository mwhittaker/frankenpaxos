// TODO(mwhittaker): There is a state machine API in frankenpaxos.statemachine.
// This state machine API is currently specialized for EPaxos, but should be
// moved to frankenpaxos.statemachine once we're confident it is useful for
// other protocols as well. The file should also be split up into multiple
// files.

package frankenpaxos.epaxos

import scala.collection.mutable
import scala.scalajs.js.annotation._

// Conflict Index //////////////////////////////////////////////////////////////

// Two state machine commands x and y conflict if there exists a state in which
// x and y do not commute; i.e. there exists a state in which executing x and
// then y does not produce the same final state and outputs as executing y and
// then x.
//
// Consider a key-value store state machine for example. The commands set(x, 1)
// and set(y, 1) do _not_ conflict because in every single state, executing
// set(x, 1) and then set(y, 1) is indistinguishable from executing set(y, 1)
// and then set(x, 1). On the other hand, the commands set(x, 1) and set(x, 2)
// do conflict. Executing set(x, 1) and then set(x, 2) leaves x with value 2
// while executing set(x, 2) and then set(x, 1) leaves x with value 1.
//
// Many consensus protocols (e.g., EPaxos, BPaxos) receive commands over time
// and assign each command a unique key (e.g., every command in EPaxos is
// assigned a particular instance). Moreover, whenever these protocols receive
// a command, they have to compute the set of already received commands that
// conflict with it. For example, imagine a consensus protocol receives the
// following stream of commands:
//
//   - key 1: set(x, 1)
//   - key 2: set(y, 1)
//   - key 3: get(x)
//   - key 4: get(y)
//
// and then receives command set(x, 2). The protocol computes the set of
// commands that conflict with set(x, 2). Here, the conflicting commands are
// set(x, 1) (key 1) and get(x) (key 3).
//
// ConflictIndex is an abstraction that can be used to efficiently compute sets
// of commands that conflict with a particular command. You can think of a
// ConflictIndex like a Map from keys to commands. You can put, get, and remove
// commands from a ConflictIndex. What differentiates a ConflictIndex from a
// Map is the getConflicts method which returns the set of all commands
// (technically, their keys) that conflict with a command. Read through a
// couple of the ConflictIndex implementations below to get a better sense for
// the API.
trait ConflictIndex[Key, Command] {
  // Put, get, and remove follow Scala's Map API [1].
  //
  // [1]: scala-lang.org/api/current/scala/collection/mutable/Map.html
  def put(key: Key, command: Command): Option[Command]
  def get(key: Key): Option[Command]
  def remove(key: Key): Option[Command]

  // `getConflicts(key, command)` returns the set of all keys in the conflict
  // index that map to commands that conflict with `command`. Note that `key`
  // is never returned, even if it is in the conflict index.
  def getConflicts(key: Key, command: Command): Set[Key]
}

class NaiveConflictIndex[Key, Command](
    conflict: (Command, Command) => Boolean
) extends ConflictIndex[Key, Command] {
  private val commands = mutable.Map[Key, Command]()

  override def put(key: Key, command: Command): Option[Command] =
    commands.put(key, command)

  override def get(key: Key): Option[Command] =
    commands.get(key)

  override def remove(key: Key): Option[Command] =
    commands.remove(key)

  override def getConflicts(key: Key, command: Command): Set[Key] = {
    commands
      .filter({ case (k, c) => k != key && conflict(c, command) })
      .keys
      .to[Set]
  }
}

// State Machine API ///////////////////////////////////////////////////////////

// TODO(mwhittaker): Document this API once we merge with the code in
// frankenpaxos.statemachine.
trait StateMachine {
  // Abstract value members.
  def run(input: Array[Byte]): Array[Byte]
  def conflicts(firstCommand: Array[Byte], secondCommand: Array[Byte]): Boolean

  // Concrete value members.
  def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] =
    new NaiveConflictIndex(conflicts)
}

// TODO(mwhittaker): Document this API once we merge with the code in
// frankenpaxos.statemachine.
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

      override def getConflicts(key: Key, command: Array[Byte]): Set[Key] = {
        index
          .getConflicts(key, inputSerializer.fromBytes(command))
      }
    }
  }

  def typedConflictIndex[Key](): ConflictIndex[Key, I] =
    new NaiveConflictIndex(typedConflicts)
}

// Example State Machines //////////////////////////////////////////////////////

class Register extends StateMachine {
  private var x: String = ""

  override def toString(): String = x

  override def run(input: Array[Byte]): Array[Byte] = {
    x = new String(input)
    input
  }

  // We say every pair of commands conflict. Technically, if two strings are
  // the same, they don't conflict, but to keep things simple, we say
  // everything conflicts.
  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = true

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] = {
    new ConflictIndex[Key, Array[Byte]] {
      private val commands = mutable.Map[Key, Array[Byte]]()

      override def put(key: Key, command: Array[Byte]): Option[Array[Byte]] =
        commands.put(key, command)

      override def get(key: Key): Option[Array[Byte]] =
        commands.get(key)

      override def remove(key: Key): Option[Array[Byte]] =
        commands.remove(key)

      // Since every pair of commands conflict, we return every key except for
      // `key`.
      override def getConflicts(key: Key, command: Array[Byte]): Set[Key] =
        commands.filterKeys(_ != key).keys.to[Set]
    }
  }
}

@JSExportAll
class KeyValueStore
    extends TypedStateMachine[KeyValueStoreInput, KeyValueStoreOutput] {
  private val kvs = mutable.Map[String, String]()

  // TODO(mwhittaker): Remove.
  var executedCommands: mutable.ListBuffer[KeyValueStoreInput] =
    mutable.ListBuffer[KeyValueStoreInput]()

  override def toString(): String = kvs.toString()

  object KeyValueStoreInputSerializer
      extends frankenpaxos.ProtoSerializer[KeyValueStoreInput]
  object KeyValueStoreOutputSerializer
      extends frankenpaxos.ProtoSerializer[KeyValueStoreOutput]
  override val inputSerializer = KeyValueStoreInputSerializer
  override val outputSerializer = KeyValueStoreOutputSerializer

  override def typedRun(input: KeyValueStoreInput): KeyValueStoreOutput = {
    // TODO(mwhittaker): Remove.
    executedCommands.append(input)

    import KeyValueStoreInput.Request
    input.request match {
      case Request.GetRequest(GetRequest(keys)) =>
        KeyValueStoreOutput().withGetReply(
          GetReply(keys.map(k => GetKeyValuePair(k, kvs.get(k))))
        )

      case Request.SetRequest(SetRequest(keyValues)) =>
        keyValues.foreach({ case SetKeyValuePair(k, v) => kvs(k) = v })
        KeyValueStoreOutput().withSetReply(SetReply())

      case Request.Empty =>
        throw new IllegalStateException()
    }
  }

  private def keys(input: KeyValueStoreInput): Set[String] = {
    import KeyValueStoreInput.Request
    input.request match {
      case Request.GetRequest(GetRequest(keys)) =>
        keys.to[Set]
      case Request.SetRequest(SetRequest(keyValues)) =>
        keyValues.map({ case SetKeyValuePair(k, _) => k }).to[Set]
      case Request.Empty =>
        throw new IllegalStateException()
    }
  }

  override def typedConflicts(
      firstCommand: KeyValueStoreInput,
      secondCommand: KeyValueStoreInput
  ): Boolean = {
    import KeyValueStoreInput.Request

    (firstCommand.request, secondCommand.request) match {
      case (Request.GetRequest(_), Request.GetRequest(_)) =>
        // Get requests do not conflict.
        false

      case (Request.SetRequest(_), Request.GetRequest(_)) |
          (Request.GetRequest(_), Request.SetRequest(_)) |
          (Request.SetRequest(_), Request.SetRequest(_)) =>
        keys(firstCommand).intersect(keys(secondCommand)).nonEmpty

      case (Request.Empty, _) | (_, Request.Empty) =>
        throw new IllegalStateException()
    }
  }

  override def typedConflictIndex[CommandKey]()
    : ConflictIndex[CommandKey, KeyValueStoreInput] = {
    new ConflictIndex[CommandKey, KeyValueStoreInput] {
      // Commands maps command keys to commands. gets and sets are inverted
      // indexes that record the commands that get or set a particular key. For
      // example, if we put command `get(x), get(y)` with key 1 and put command
      // `set(y), set(z)` with command 2, then commands, gets, and sets, look
      // like this.
      //
      //   commands | 1 | get(x), get(y) |
      //            | 2 | set(y), set(z) |
      //
      //   gets     | x | 1 |
      //            | y | 1 |
      //
      //   sets     | y | 2 |
      //            | z | 2 |
      private val commands = mutable.Map[CommandKey, KeyValueStoreInput]()
      private val gets = mutable.Map[String, mutable.Set[CommandKey]]()
      private val sets = mutable.Map[String, mutable.Set[CommandKey]]()

      override def put(
          commandKey: CommandKey,
          command: KeyValueStoreInput
      ): Option[KeyValueStoreInput] = {
        val o = remove(commandKey)
        commands(commandKey) = command

        import KeyValueStoreInput.Request
        command.request match {
          case Request.GetRequest(GetRequest(keys)) =>
            for (key <- keys) {
              gets.getOrElseUpdate(key, mutable.Set())
              gets(key) += commandKey
            }

          case Request.SetRequest(SetRequest(keyValues)) =>
            for (SetKeyValuePair(key, _) <- keyValues) {
              sets.getOrElseUpdate(key, mutable.Set())
              sets(key) += commandKey
            }

          case Request.Empty =>
            throw new IllegalStateException()
        }

        o
      }

      override def get(commandKey: CommandKey): Option[KeyValueStoreInput] =
        commands.get(commandKey)

      override def remove(
          commandKey: CommandKey
      ): Option[KeyValueStoreInput] = {
        commands.remove(commandKey) match {
          case None => None

          case Some(input) => {
            import KeyValueStoreInput.Request
            input.request match {
              case Request.GetRequest(GetRequest(keys)) =>
                for (key <- keys) {
                  gets(key) -= commandKey
                  if (gets(key).isEmpty) {
                    gets -= key
                  }
                }

              case Request.SetRequest(SetRequest(keyValues)) =>
                for (SetKeyValuePair(key, _) <- keyValues) {
                  sets(key) -= commandKey
                  if (sets(key).isEmpty) {
                    sets -= key
                  }
                }

              case Request.Empty =>
                throw new IllegalStateException()
            }
            Some(input)
          }
        }
      }

      override def getConflicts(
          commandKey: CommandKey,
          command: KeyValueStoreInput
      ): Set[CommandKey] = {
        import KeyValueStoreInput.Request
        val commandKeys = command.request match {
          case Request.GetRequest(GetRequest(keys)) =>
            keys
              .to[Set]
              .map(key => sets.getOrElse(key, mutable.Set()).to[Set])
              .flatten

          case Request.SetRequest(SetRequest(keyValues)) =>
            val keys = keyValues.map({ case SetKeyValuePair(k, v) => k })
            val getCommandKeys =
              keys
                .to[Set]
                .map(key => gets.getOrElse(key, mutable.Set()).to[Set])
                .flatten
            val setCommandKeys =
              keys
                .to[Set]
                .map(key => sets.getOrElse(key, mutable.Set()).to[Set])
                .flatten
            getCommandKeys.union(setCommandKeys)

          case Request.Empty =>
            throw new IllegalStateException()
        }
        commandKeys - commandKey
      }
    }
  }
}
