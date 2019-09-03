package frankenpaxos.util

import scala.collection.mutable
import scala.scalajs.js.annotation._

// TODO(mwhittaker): Document.
@JSExportAll
class BufferMap[V](val growSize: Int = 5000) {
  @JSExport
  protected val buffer: mutable.Buffer[Option[V]] =
    mutable.Buffer.fill(growSize)(None)

  @JSExport
  protected var watermark: Int = 0

  override def toString(): String = buffer.toString()

  private def normalize(key: Int): Int = key - watermark

  private def pad(len: Int) {
    while (buffer.size < len) {
      buffer += None
    }
  }

  def get(key: Int): Option[V] = {
    if (normalize(key) < 0 || normalize(key) >= buffer.size) {
      None
    } else {
      buffer(normalize(key))
    }
  }

  def put(key: Int, value: V): Unit = {
    if (normalize(key) < 0) {
      return
    }

    if (normalize(key) < buffer.size) {
      buffer(normalize(key)) = Some(value)
      return
    }

    pad(normalize(key) + 1 + growSize)
    buffer(normalize(key)) = Some(value)
  }

  def garbageCollect(watermark: Int): Unit = {
    if (watermark <= this.watermark) {
      return
    }

    buffer.remove(0, Math.min(watermark - this.watermark, buffer.size))
    this.watermark = watermark
  }

  // Converts this BufferMap into a normal map. All garbage collected entries
  // are not returned in the map. This should really only be used for testing.
  // It's not designed to be efficient.
  def toMap(): Map[Int, V] = {
    buffer.zipWithIndex
      .flatMap({
        case (Some(v), i) => Some(i + watermark, v)
        case (None, i)    => None
      })
      .toMap
  }
}
