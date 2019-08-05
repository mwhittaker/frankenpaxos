package frankenpaxos.statemachine

import collection.mutable
import org.scalacheck.Gen
import scala.scalajs.js.annotation._

object KeyValueStoreInputSerializer
    extends frankenpaxos.ProtoSerializer[KeyValueStoreInput]

object KeyValueStoreOutputSerializer
    extends frankenpaxos.ProtoSerializer[KeyValueStoreOutput]

@JSExportAll
object KeyValueStore {
  def getOneOf(keys: Seq[String]): Gen[KeyValueStoreInput] = {
    for {
      key <- Gen.oneOf(keys)
    } yield {
      KeyValueStoreInput().withGetRequest(GetRequest(key = Seq(key)))
    }
  }

  def setOneOf(keyValues: Seq[(String, String)]): Gen[KeyValueStoreInput] = {
    for {
      (key, value) <- Gen.oneOf(keyValues)
    } yield {
      KeyValueStoreInput().withSetRequest(
        SetRequest(keyValue = Seq(SetKeyValuePair(key, value)))
      )
    }
  }
}

@JSExportAll
class KeyValueStore
    extends TypedStateMachine[KeyValueStoreInput, KeyValueStoreOutput] {
  private val kvs = mutable.Map[String, String]()

  override def toString(): String = kvs.toString()
  override val inputSerializer = KeyValueStoreInputSerializer
  override val outputSerializer = KeyValueStoreOutputSerializer

  override def typedRun(input: KeyValueStoreInput): KeyValueStoreOutput = {
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

      override def filterInPlace(
          f: (CommandKey, KeyValueStoreInput) => Boolean
      ): this.type = {
        for ((commandKey, command) <- commands) {
          if (f(commandKey, command)) {
            remove(commandKey)
          }
        }
        this
      }

      override def getConflicts(
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
        commandKeys
      }
    }
  }
}
