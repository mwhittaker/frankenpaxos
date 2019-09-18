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
      KeyValueStoreInput(
        batch =
          Seq(KeyValueStoreRequest().withGetRequest(GetRequest(key = key)))
      )
    }
  }

  def setOneOf(keyValues: Seq[(String, String)]): Gen[KeyValueStoreInput] = {
    for {
      (key, value) <- Gen.oneOf(keyValues)
    } yield {
      KeyValueStoreInput(
        batch = Seq(
          KeyValueStoreRequest().withSetRequest(
            SetRequest(key = key, value = value)
          )
        )
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

  private def typedRun(input: KeyValueStoreRequest): KeyValueStoreReply = {
    import KeyValueStoreRequest.Request
    input.request match {
      case Request.GetRequest(GetRequest(key)) =>
        KeyValueStoreReply().withGetReply(GetReply(kvs.get(key)))

      case Request.SetRequest(SetRequest(key, value)) =>
        kvs(key) = value
        KeyValueStoreReply().withSetReply(SetReply())

      case Request.Empty =>
        throw new IllegalStateException()
    }
  }

  override def typedRun(input: KeyValueStoreInput): KeyValueStoreOutput = {
    KeyValueStoreOutput(input.batch.map(typedRun))
  }

  private def keys(
      input: KeyValueStoreInput
  ): (mutable.Set[String], mutable.Set[String]) = {
    val getKeys = mutable.Set[String]()
    val setKeys = mutable.Set[String]()
    for (request <- input.batch) {
      import KeyValueStoreRequest.Request
      request.request match {
        case Request.GetRequest(GetRequest(key)) =>
          getKeys += key
        case Request.SetRequest(SetRequest(key, value)) =>
          setKeys += key
        case Request.Empty =>
          throw new IllegalStateException()
      }
    }
    (getKeys, setKeys)
  }

  private def overlap[A](xs: mutable.Set[A], ys: mutable.Set[A]): Boolean = {
    // Emperically faster than xs.intersect(ys).isEmpty.
    if (xs.size < ys.size) {
      xs.exists(ys.contains(_))
    } else {
      ys.exists(xs.contains(_))
    }
  }

  override def typedConflicts(
      firstCommand: KeyValueStoreInput,
      secondCommand: KeyValueStoreInput
  ): Boolean = {
    val (firstGetKeys, firstSetKeys) = keys(firstCommand)
    val (secondGetKeys, secondSetKeys) = keys(secondCommand)
    overlap(firstGetKeys, secondSetKeys) ||
    overlap(firstSetKeys, secondGetKeys) ||
    overlap(firstSetKeys, secondSetKeys)
  }

  override def toBytes(): Array[Byte] =
    KeyValueStoreProto(
      kvs.iterator
        .map({ case (k, v) => KeyValuePair(key = k, value = v) })
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
      // TODO(mwhittaker): Update with snapshots.

      override def put(
          commandKey: CommandKey,
          command: KeyValueStoreInput
      ): Unit = {
        remove(commandKey)
        commands(commandKey) = command

        for (request <- command.batch) {
          import KeyValueStoreRequest.Request
          request.request match {
            case Request.GetRequest(GetRequest(key)) =>
              gets.getOrElseUpdate(key, mutable.Set()) += commandKey

            case Request.SetRequest(SetRequest(key, value)) =>
              sets.getOrElseUpdate(key, mutable.Set()) += commandKey

            case Request.Empty =>
              throw new IllegalStateException()
          }
        }
      }

      override def putSnapshot(commandKey: CommandKey): Unit =
        // TODO(mwhittaker): Update with snapshots.
        ???

      override def remove(
          commandKey: CommandKey
      ): Unit = {
        commands.remove(commandKey) match {
          case None =>
            None

          case Some(input) => {
            for (request <- input.batch) {
              import KeyValueStoreRequest.Request
              request.request match {
                case Request.GetRequest(GetRequest(key)) =>
                  gets(key) -= commandKey
                  if (gets(key).isEmpty) {
                    gets -= key
                  }

                case Request.SetRequest(SetRequest(key, value)) =>
                  sets(key) -= commandKey
                  if (sets(key).isEmpty) {
                    sets -= key
                  }

                case Request.Empty =>
                  throw new IllegalStateException()
              }
            }
          }
        }
      }

      override def getConflicts(
          command: KeyValueStoreInput
      ): Set[CommandKey] = {
        val conflicts = mutable.Set[CommandKey]()
        val (getKeys, setKeys) = keys(command)
        for (key <- getKeys) {
          conflicts ++= sets.getOrElse(key, mutable.Set())
        }
        for (key <- setKeys) {
          conflicts ++= sets.getOrElse(key, mutable.Set())
          conflicts ++= gets.getOrElse(key, mutable.Set())
        }
        conflicts.toSet
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
        // TODO(mwhittaker): Update with snapshots.

        override def put(
            commandKey: CommandKey,
            command: KeyValueStoreInput
        ): Unit = {
          for (request <- command.batch) {
            import KeyValueStoreRequest.Request
            request.request match {
              case Request.GetRequest(GetRequest(key)) =>
                gets
                  .getOrElseUpdate(key, new TopOne(numLeaders, like))
                  .put(commandKey)

              case Request.SetRequest(SetRequest(key, value)) =>
                sets
                  .getOrElseUpdate(key, new TopOne(numLeaders, like))
                  .put(commandKey)

              case Request.Empty =>
                throw new IllegalStateException()
            }
          }
        }

        override def putSnapshot(commandKey: CommandKey): Unit =
          // TODO(mwhittaker): Update with snapshots.
          ???

        override def getTopOneConflicts(
            command: KeyValueStoreInput
        ): TopOne[CommandKey] = {
          val (getKeys, setKeys) = keys(command)
          val merged = new TopOne(numLeaders, like)
          for (key <- getKeys) {
            sets.get(key).foreach(merged.mergeEquals(_))
          }
          for (key <- setKeys) {
            sets.get(key).foreach(merged.mergeEquals(_))
            gets.get(key).foreach(merged.mergeEquals(_))
          }
          merged
        }
      }
    } else {
      new ConflictIndex[CommandKey, KeyValueStoreInput] {
        type LeaderIndex = Int
        private val gets = mutable.Map[String, TopK[CommandKey]]()
        private val sets = mutable.Map[String, TopK[CommandKey]]()
        // TODO(mwhittaker): Update with snapshots.

        override def put(
            commandKey: CommandKey,
            command: KeyValueStoreInput
        ): Unit = {
          for (request <- command.batch) {
            import KeyValueStoreRequest.Request
            request.request match {
              case Request.GetRequest(GetRequest(key)) =>
                gets
                  .getOrElseUpdate(key, new TopK(k, numLeaders, like))
                  .put(commandKey)

              case Request.SetRequest(SetRequest(key, value)) =>
                sets
                  .getOrElseUpdate(key, new TopK(k, numLeaders, like))
                  .put(commandKey)

              case Request.Empty =>
                throw new IllegalStateException()
            }
          }
        }

        override def putSnapshot(commandKey: CommandKey): Unit =
          // TODO(mwhittaker): Update with snapshots.
          ???

        override def getTopKConflicts(
            command: KeyValueStoreInput
        ): TopK[CommandKey] = {
          val (getKeys, setKeys) = keys(command)
          val merged = new TopK(k, numLeaders, like)
          for (key <- getKeys) {
            sets.get(key).foreach(merged.mergeEquals(_))
          }
          for (key <- setKeys) {
            sets.get(key).foreach(merged.mergeEquals(_))
            gets.get(key).foreach(merged.mergeEquals(_))
          }
          merged
        }
      }
    }
  }
}
