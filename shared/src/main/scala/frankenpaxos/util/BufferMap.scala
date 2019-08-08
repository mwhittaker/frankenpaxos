package frankenpaxos.util

import scala.collection.mutable

// TODO(mwhittaker): Document.
class BufferMap[V](val growSize: Int = 5000) {
  private val buffer: mutable.Buffer[Option[V]] =
    mutable.Buffer.fill(growSize)(None)
  private var watermark: Int = 0

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
}
