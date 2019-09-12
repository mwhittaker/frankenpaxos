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
      @JSExport
      protected val commandsAndSnapshots = mutable.Set[Key]()

      override def put(key: Key, command: Array[Byte]): Unit =
        commandsAndSnapshots += key

      override def putSnapshot(key: Key): Unit =
        commandsAndSnapshots += key

      override def remove(key: Key): Unit =
        commandsAndSnapshots -= key

      // Since every pair of commands conflict, we return every key.
      override def getConflicts(command: Array[Byte]): Set[Key] =
        commandsAndSnapshots.toSet

      // Since every pair of commands conflict, we return every key.
      override def getSnapshotConflicts(): Set[Key] =
        commandsAndSnapshots.toSet
    }

  override def topKConflictIndex[Key](
      k: Int,
      like: VertexIdLike[Key]
  ): ConflictIndex[Key, Array[Byte]] = {
    if (k == 1) {
      new ConflictIndex[Key, Array[Byte]] {
        private val topOne = new TopOne(like)

        override def put(key: Key, command: Array[Byte]): Unit = topOne.put(key)
        override def putSnapshot(key: Key): Unit = topOne.put(key)
        override def remove(key: Key): Unit =
          throw new java.lang.UnsupportedOperationException()
        override def getConflicts(command: Array[Byte]): Set[Key] = topOne.get()
        override def getSnapshotConflicts(): Set[Key] = topOne.get()
      }
    } else {
      new ConflictIndex[Key, Array[Byte]] {
        private val topK = new TopK[Key](k, like)

        override def put(key: Key, command: Array[Byte]): Unit = topK.put(key)
        override def putSnapshot(key: Key): Unit = topK.put(key)
        override def remove(key: Key): Unit =
          throw new java.lang.UnsupportedOperationException()
        override def getConflicts(command: Array[Byte]): Set[Key] = topK.get()
        override def getSnapshotConflicts(): Set[Key] = topK.get()
      }
    }
  }
}
