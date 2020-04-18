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

  @JSExport
  protected var largestKey: Int = -1

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
    largestKey = Math.max(largestKey, key)

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

  def contains(key: Int): Boolean = get(key).isDefined

  def garbageCollect(watermark: Int): Unit = {
    if (watermark <= this.watermark) {
      return
    }

    buffer.remove(0, Math.min(watermark - this.watermark, buffer.size))
    this.watermark = watermark
  }

  // An iterator that iterates from index `startInclusive` to `endExclusive`.
  // If `endExclusive` is -1, then the iterator is empty.
  class BufferMapIterator(startInclusive: Int, endInclusive: Int)
      extends Iterator[(Int, V)] {
    private var index: Int = startInclusive

    private def seekToNext(): Option[(Int, V)] = {
      while (index <= endInclusive) {
        get(index) match {
          case None =>
            index += 1
          case Some(v) =>
            index += 1
            return Some(index - 1, v)
        }
      }
      None
    }

    private var nextValue: Option[(Int, V)] = seekToNext()

    override def hasNext: Boolean = nextValue.isDefined

    override def next(): (Int, V) = {
      nextValue match {
        case None =>
          throw new NoSuchElementException()
        case Some(v) =>
          nextValue = seekToNext()
          v
      }
    }
  }

  def iterator(): Iterator[(Int, V)] =
    new BufferMapIterator(0, largestKey)

  def iteratorFrom(key: Int): Iterator[(Int, V)] =
    new BufferMapIterator(key, largestKey)

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
