package frankenpaxos.statemachine

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class AppendLog extends StateMachine {
  private var xs = mutable.Buffer[String]()

  override def toString(): String = xs.toString()

  def get(): Seq[String] = xs.toSeq

  override def run(input: Array[Byte]): Array[Byte] = {
    xs += new String(input)
    (xs.size - 1).toString().getBytes()
  }

  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = true

  override def toBytes(): Array[Byte] =
    AppendLogProto(x = xs).toByteArray

  override def fromBytes(snapshot: Array[Byte]): Unit = {
    xs = AppendLogProto.parseFrom(snapshot).x.to[mutable.Buffer]
  }

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] =
    new ConflictIndex[Key, Array[Byte]] {
      private val xs = mutable.Map[Key, Array[Byte]]()
      override def toString(): String = xs.toString()
      override def put(key: Key, command: Array[Byte]): Option[Array[Byte]] =
        xs.put(key, command)
      override def remove(key: Key): Option[Array[Byte]] = xs.remove(key)
      override def getConflicts(command: Array[Byte]): Set[Key] = xs.keys.toSet
    }

  override def topKConflictIndex[Key](
      k: Int,
      like: VertexIdLike[Key]
  ): ConflictIndex[Key, Array[Byte]] = {
    if (k == 1) {
      new ConflictIndex[Key, Array[Byte]] {
        type LeaderIndex = Int
        private val commands = mutable.Map[LeaderIndex, Key]()

        override def put(
            key: Key,
            command: Array[Byte]
        ): Option[Array[Byte]] = {
          val leaderIndex = like.leaderIndex(key)
          commands.get(leaderIndex) match {
            case None => commands(leaderIndex) = key

            case Some(largestKey) =>
              if (leaderIndex > like.leaderIndex(largestKey)) {
                commands(leaderIndex) = key
              }
          }
          None
        }

        override def remove(key: Key): Option[Array[Byte]] = ???

        override def getConflicts(command: Array[Byte]): Set[Key] =
          commands.values.toSet
      }
    } else {
      new ConflictIndex[Key, Array[Byte]] {
        type LeaderIndex = Int
        private val commands =
          mutable.Map[LeaderIndex, mutable.SortedSet[Key]]()

        override def put(
            key: Key,
            command: Array[Byte]
        ): Option[Array[Byte]] = {
          val leaderIndex = like.leaderIndex(key)
          commands.get(leaderIndex) match {
            case None =>
              commands(leaderIndex) =
                mutable.SortedSet[Key](key)(like.intraLeaderOrdering)

            case Some(keys) =>
              keys += key
              if (keys.size > k) {
                keys -= keys.firstKey
              }
          }
          None
        }

        override def remove(key: Key): Option[Array[Byte]] = ???

        override def getConflicts(command: Array[Byte]): Set[Key] =
          commands.values.flatten.toSet
      }
    }
  }
}
