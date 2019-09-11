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

  class TopOne[T](like: VertexIdLike[T]) {
    type LeaderIndex = Int
    val topOnes = mutable.Map[LeaderIndex, T]()

    def put(x: T): Unit = {
      topOnes.get(like.leaderIndex(x)) match {
        case Some(y) =>
          if (like.id(x) > like.id(y)) {
            topOnes(like.leaderIndex(x)) = x
          }
        case None =>
          topOnes(like.leaderIndex(x)) = x
      }
    }

    def get(): Set[T] = topOnes.values.toSet

    def mergeEquals(other: TopOne[T]): Unit = {
      for ((leaderIndex, y) <- other.topOnes) {
        topOnes.get(leaderIndex) match {
          case None => topOnes(leaderIndex) = y
          case Some(x) =>
            if (like.id(y) > like.id(x)) {
              topOnes(leaderIndex) = y
            }
        }
      }
    }
  }

  class TopK[T](k: Int, like: VertexIdLike[T]) {
    type LeaderIndex = Int
    val topOnes = mutable.Map[LeaderIndex, mutable.SortedSet[T]]()

    def put(x: T): Unit = {
      topOnes.get(like.leaderIndex(x)) match {
        case Some(ys) =>
          ys += x
          if (ys.size > k) {
            ys.remove(ys.head)
          }
        case None =>
          topOnes(like.leaderIndex(x)) =
            mutable.SortedSet(x)(like.intraLeaderOrdering)
      }
    }

    def get(): Set[T] = topOnes.values.flatten.toSet

    def mergeEquals(other: TopK[T]): Unit = {
      for ((leaderIndex, ys) <- other.topOnes) {
        topOnes.get(leaderIndex) match {
          case None =>
            val xs = mutable.SortedSet()(like.intraLeaderOrdering)
            topOnes(leaderIndex) = xs
            xs ++= ys
          case Some(xs) =>
            xs ++= ys
            while (xs.size > k) {
              xs.remove(xs.head)
            }
        }
      }
    }
  }

  def typedTopKConflictIndex[CommandKey](
      k: Int,
      like: VertexIdLike[CommandKey]
  ): ConflictIndex[CommandKey, KeyValueStoreInput] = {
    if (k == 1) {
      new ConflictIndex[CommandKey, KeyValueStoreInput] {
        type LeaderIndex = Int
        private val gets = mutable.Map[String, TopOne[CommandKey]]()
        private val sets = mutable.Map[String, TopOne[CommandKey]]()

        override def put(
            commandKey: CommandKey,
            command: KeyValueStoreInput
        ): Option[KeyValueStoreInput] = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              for (key <- keys) {
                gets.getOrElseUpdate(key, new TopOne(like)).put(commandKey)
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              for (SetKeyValuePair(key, _) <- keyValues) {
                sets.getOrElseUpdate(key, new TopOne(like)).put(commandKey)
              }

            case Request.Empty =>
              throw new IllegalStateException()
          }
          None
        }

        override def remove(
            commandKey: CommandKey
        ): Option[KeyValueStoreInput] = ???

        override def getConflicts(
            command: KeyValueStoreInput
        ): Set[CommandKey] = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              if (keys.size == 0) {
                Set()
              } else {
                val merged = new TopOne(like)
                for (key <- keys) {
                  sets.get(key) match {
                    case None         =>
                    case Some(topOne) => merged.mergeEquals(topOne)
                  }
                }
                merged.get()
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              if (keyValues.size == 0) {
                Set()
              } else {
                val merged = new TopOne(like)
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
                merged.get()
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

        override def put(
            commandKey: CommandKey,
            command: KeyValueStoreInput
        ): Option[KeyValueStoreInput] = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              for (key <- keys) {
                gets.getOrElseUpdate(key, new TopK(k, like)).put(commandKey)
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              for (SetKeyValuePair(key, _) <- keyValues) {
                sets.getOrElseUpdate(key, new TopK(k, like)).put(commandKey)
              }

            case Request.Empty =>
              throw new IllegalStateException()
          }
          None
        }

        override def remove(
            commandKey: CommandKey
        ): Option[KeyValueStoreInput] = ???

        override def getConflicts(
            command: KeyValueStoreInput
        ): Set[CommandKey] = {
          import KeyValueStoreInput.Request
          command.request match {
            case Request.GetRequest(GetRequest(keys)) =>
              if (keys.size == 0) {
                Set()
              } else {
                val merged = new TopK(k, like)
                for (key <- keys) {
                  sets.get(key) match {
                    case None       =>
                    case Some(topK) => merged.mergeEquals(topK)
                  }
                }
                merged.get()
              }

            case Request.SetRequest(SetRequest(keyValues)) =>
              if (keyValues.size == 0) {
                Set()
              } else {
                val merged = new TopK(k, like)
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
                merged.get()
              }

            case Request.Empty =>
              throw new IllegalStateException()
          }
        }
      }
    }
  }
}
