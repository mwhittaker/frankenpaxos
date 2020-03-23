package frankenpaxos.statemachine

import frankenpaxos.util.TopK
import frankenpaxos.util.TopOne
import frankenpaxos.util.VertexIdLike
import scala.collection.mutable
import scala.scalajs.js.annotation._

// A ReadableAppendLog is a log of non-empty strings. If an empty string is
// received, the latest entry in the log is returned. This API is a little
// janky, but it's meant to keep testing simple.
@JSExportAll
class ReadableAppendLog extends StateMachine {
  private var xs = mutable.Buffer[String]()

  override def toString(): String = xs.toString()

  def get(): Seq[String] = xs.toSeq

  override def run(input: Array[Byte]): Array[Byte] = {
    if (input.size > 0) {
      xs += new String(input)
      (xs.size - 1).toString().getBytes()
    } else {
      if (xs.size == 0) {
        Array[Byte]()
      } else {
        xs(xs.size - 1).getBytes
      }
    }
  }

  override def conflicts(
      firstCommand: Array[Byte],
      secondCommand: Array[Byte]
  ): Boolean = ???

  override def toBytes(): Array[Byte] =
    AppendLogProto(x = xs).toByteArray

  override def fromBytes(snapshot: Array[Byte]): Unit = {
    xs = AppendLogProto.parseFrom(snapshot).x.to[mutable.Buffer]
  }

  override def conflictIndex[Key](): ConflictIndex[Key, Array[Byte]] =
    ???

  override def topKConflictIndex[Key](
      k: Int,
      numLeaders: Int,
      like: VertexIdLike[Key]
  ): ConflictIndex[Key, Array[Byte]] =
    ???
}
