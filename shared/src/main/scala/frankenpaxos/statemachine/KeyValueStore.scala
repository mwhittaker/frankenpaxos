package frankenpaxos.statemachine

import collection.mutable
import frankenpaxos.util.TopK
import frankenpaxos.util.TopOne
import frankenpaxos.util.VertexIdLike
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

  def get(): Map[String, String] = kvs.toMap

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

  override def toBytes(): Array[Byte] =
    KeyValueStoreProto(
      kvs.iterator
        .map({ case (k, v) => SetKeyValuePair(key = k, value = v) })
        .toSeq
    ).toByteArray

  override def fromBytes(snapshot: Array[Byte]): Unit = {
    kvs.clear()
    for (kv <- KeyValueStoreProto.parseFrom(snapshot).kv) {
      kvs(kv.key) = kv.value
    }
  }

  override def typedConflictIndex[CommandKey]()
    : ConflictIndex[CommandKey, KeyValueStoreInput] =
    new ConflictIndex[CommandKey, KeyValueStoreInput] {
      // Commands maps command keys to commands. gets and sets are inverted
      // indexes that record the commands that get or set a particular key. For
      // example, if we put command `get(x), get(y)` with key 1 and put command
      // `set(y), set(z)` with command 2, then commands, gets, and sets, look
      // like this. `snapshots` stores the set of snapshots.
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
      private val snapshots = mutable.Set[CommandKey]()

      override def put(
          commandKey: CommandKey,
          command: KeyValueStoreInput
      ): Unit = {
        remove(commandKey)
        commands(commandKey) = command

        import KeyValueStoreInput.Request
        command.request match {
          case Request.GetRequest(GetRequest(keys)) =>
            for (key <- keys) {
              gets.getOrElseUpdate(key, mutable.Set()) += commandKey
            }

          case Request.SetRequest(SetRequest(keyValues)) =>
            for (SetKeyValuePair(key, _) <- keyValues) {
              sets.getOrElseUpdate(key, mutable.Set()) += commandKey
            }

          case Request.Empty =>
            throw new IllegalStateException()
        }
      }

      override def putSnapshot(commandKey: CommandKey): Unit =
        snapshots += commandKey

      override def remove(
          commandKey: CommandKey
      ): Unit = {
        commands.remove(commandKey) match {
          case None =>
            None

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
          }
        }
      }

      override def getConflicts(
          command: KeyValueStoreInput
      ): Set[CommandKey] = {
        import KeyValueStoreInput.Request
        val commandKeys = command.request match {
          case Request.GetRequest(GetRequest(keys)) =>
            val setConflicts =
              keys.map(key => sets.getOrElse(key, mutable.Set())).flatten
            (setConflicts ++ snapshots).toSet

          case Request.SetRequest(SetRequest(keyValues)) =>
            val keys = keyValues.map({ case SetKeyValuePair(k, v) => k })
            val getConflicts =
              keys.map(key => gets.getOrElse(key, mutable.Set()))
            val setConflicts =
              keys.map(key => sets.getOrElse(key, mutable.Set()))
            ((getConflicts ++ setConflicts).flatten ++ snapshots).toSet

          case Request.Empty =>
            throw new IllegalStateException()
        }
        commandKeys
      }
    }

  def typedTopKConflictIndex[CommandKey](
      k: Int,
      numLeaders: Int,
      like: VertexIdLike[CommandKey]
  ): ConflictIndex[CommandKey, KeyValueStoreInput] = {
    if (k == 1) {
      new ConflictIndex[CommandKey, KeyValueStoreInput] {
        type LeaderIndex = Int
        private val gets = mutable.Map[String, TopOne[CommandKey]]()
        private val sets = mutable.Map[String, TopOne[CommandKey]]()
        private val snapshots = new TopOne[CommandKey](numLeaders, like)

        override def put(
            commandKey: CommandKey,
            command: KeyValueStoreInput
        ): Unit = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              for (key <- keys) {
                gets
                  .getOrElseUpdate(key, new TopOne(numLeaders, like))
                  .put(commandKey)
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              for (SetKeyValuePair(key, _) <- keyValues) {
                sets
                  .getOrElseUpdate(key, new TopOne(numLeaders, like))
                  .put(commandKey)
              }

            case Request.Empty =>
              throw new IllegalStateException()
          }
        }

        override def putSnapshot(commandKey: CommandKey): Unit =
          snapshots.put(commandKey)

        override def getTopOneConflicts(
            command: KeyValueStoreInput
        ): TopOne[CommandKey] = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              if (keys.size == 0) {
                snapshots
              } else {
                val merged = new TopOne(numLeaders, like)
                for (key <- keys) {
                  sets.get(key) match {
                    case None         =>
                    case Some(topOne) => merged.mergeEquals(topOne)
                  }
                }
                merged.mergeEquals(snapshots)
                merged
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              if (keyValues.size == 0) {
                snapshots
              } else {
                val merged = new TopOne(numLeaders, like)
                for (SetKeyValuePair(key, _) <- keyValues) {
                  sets.get(key) match {
                    case None         =>
                    case Some(topOne) => merged.mergeEquals(topOne)
                  }
                  gets.get(key) match {
                    case None         =>
                    case Some(topOne) => merged.mergeEquals(topOne)
                  }
                }
                merged.mergeEquals(snapshots)
                merged
              }

            case Request.Empty =>
              throw new IllegalStateException()
          }
        }
      }
    } else {
      new ConflictIndex[CommandKey, KeyValueStoreInput] {
        type LeaderIndex = Int
        private val gets = mutable.Map[String, TopK[CommandKey]]()
        private val sets = mutable.Map[String, TopK[CommandKey]]()
        private val snapshots = new TopK[CommandKey](k, numLeaders, like)

        override def put(
            commandKey: CommandKey,
            command: KeyValueStoreInput
        ): Unit = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              for (key <- keys) {
                gets
                  .getOrElseUpdate(key, new TopK(k, numLeaders, like))
                  .put(commandKey)
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              for (SetKeyValuePair(key, _) <- keyValues) {
                sets
                  .getOrElseUpdate(key, new TopK(k, numLeaders, like))
                  .put(commandKey)
              }

            case Request.Empty =>
              throw new IllegalStateException()
          }
        }

        override def putSnapshot(commandKey: CommandKey): Unit =
          snapshots.put(commandKey)

        override def getTopKConflicts(
            command: KeyValueStoreInput
        ): TopK[CommandKey] = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              if (keys.size == 0) {
                snapshots
              } else {
                val merged = new TopK(k, numLeaders, like)
                for (key <- keys) {
                  sets.get(key) match {
                    case None       =>
                    case Some(topK) => merged.mergeEquals(topK)
                  }
                }
                merged.mergeEquals(snapshots)
                merged
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              if (keyValues.size == 0) {
                snapshots
              } else {
                val merged = new TopK(k, numLeaders, like)
                for (SetKeyValuePair(key, _) <- keyValues) {
                  sets.get(key) match {
                    case None       =>
                    case Some(topK) => merged.mergeEquals(topK)
                  }
                  gets.get(key) match {
                    case None       =>
                    case Some(topK) => merged.mergeEquals(topK)
                  }
                }
                merged.mergeEquals(snapshots)
                merged
              }

            case Request.Empty =>
              throw new IllegalStateException()
          }
        }
      }
    }
  }
}
