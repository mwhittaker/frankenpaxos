package frankenpaxos.util

import scala.collection.mutable

// Assume we have a list of items numbered 0, 1, 2, 3, ... and a set n of
// machines. Each machine processes the items in increasing order starting from
// 0. At any given point in time, every machine has processed some number of
// items. For example, assume we have four machines and at one point in time,
// every machine has processed the following number of items:
//
//   machine 0: 4
//   machine 1: 3
//   machine 2: 6
//   machine 3: 2
//
// A QuorumWatermark helps us answer the question "how many items have been
// processed by k or more machines?" For example, 2 commands have been
// processed by all four machines, 3 commands have been processed by three
// machines, 4 commands have been processed by two machines, and 6 commands
// have been processed by one machine.
//
// More abstractly, we call these numbers watermarks. A QuorumWatermark
// consists of a set of `numWatermarks` watermarks, and returns the largest
// number k such that `quorumSize` or more watermarks are greater than or equal
// to k.
//
// Note that when quorumSize is equal to 1, the maximum watermark is returned,
// and when quorumSize is equal to numWatermarks, the minimum watermark is
// returned. In general, if we sort the wartermarks in descending order, the
// quorumSize'th watermark is returned (starting indexing at 1).
class QuorumWatermark(numWatermarks: Int) {
  private val watermarks: mutable.Buffer[Int] =
    mutable.Buffer.fill(numWatermarks)(0)

  override def toString(): String = s"[${watermarks.mkString(",")}]"

  // Update the ith watermark to `watermark`, unless `watermark` is smaller
  // than the current watermark. Watermarks can only increase over time.
  def update(index: Int, watermark: Int): Unit =
    watermarks(index) = Math.max(watermarks(index), watermark)

  def watermark(quorumSize: Int): Int = {
    require(quorumSize <= numWatermarks)
    require(quorumSize >= 1)
    val sorted = watermarks.sorted
    sorted(numWatermarks - quorumSize)
  }
}
