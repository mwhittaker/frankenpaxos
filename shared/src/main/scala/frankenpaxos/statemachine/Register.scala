package frankenpaxos.statemachine

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class Register extends StateMachine {
  private var x: String = ""

  override def toString(): String = x

  override def run(input: Array[Byte]): Array[Byte] = {
    x = new String(input)
    input
  }

  override def toBytes(): Array[Byte] = x.getBytes()

  override def fromBytes(snapshot: Array[Byte]): Unit = {
    x = new String(snapshot)
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

      override def remove(key: Key): Option[Array[Byte]] =
        commands.remove(key)

      // Since every pair of commands conflict, we return every key except for
      // `key`.
      override def getConflicts(command: Array[Byte]): Set[Key] =
        commands.keys.toSet
    }
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
