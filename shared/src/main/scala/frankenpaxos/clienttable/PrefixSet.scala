package frankenpaxos.clienttable

import scala.collection.mutable

// A PrefixSet is an add-only set of natural numbers (i.e., integers greater
// than or equal to 0). Because a PrefixSet is add-only and because natural
// numbers have a least element, 0, we can implement PrefixSet with a nice
// optimization. Rather than storing all the numbers in the set, we store a
// `watermark` and a set of other `values`. All numbers strictly less than
// `watermark` are in the set, and all numbers in `values` are in the set. All
// other numbers are not in the set. If numbers are inserted in roughly
// ascending contiguous order, PrefixSet should be roughly constant space.
//
//   | Set             | PrefixSet Representation     |
//   | --------------- | ---------------------------- |
//   | {}              | watermark: 0; values: {}     |
//   | {0}             | watermark: 1; values: {}     |
//   | {0, 1}          | watermark: 2; values: {}     |
//   | {0, 1, 3}       | watermark: 2; values: {3}    |
//   | {0, 1, 3, 4}    | watermark: 2; values: {3, 4} |
//   | {0, 1, 2, 3, 4} | watermark: 5; values: {}     |
class PrefixSet {
  private var watermark: Int = 0
  private val values = mutable.Set[Int]()

  override def toString(): String =
    s"{x | 0 <= x < $watermark} + $values"

  def contains(x: Int): Boolean = {
    if (x < 0) {
      throw new IllegalArgumentException(s"$x < 0.")
    }
    x < watermark || values.contains(x)
  }

  def add(x: Int): Boolean = {
    if (x < 0) {
      throw new IllegalArgumentException(s"$x < 0.")
    }

    if (x < watermark) {
      return false
    }

    val freshlyInserted = values.add(x)
    while (values.contains(watermark)) {
      values.remove(watermark)
      watermark += 1
    }
    freshlyInserted
  }

  def +(x: Int): PrefixSet = {
    add(x)
    this
  }
}
