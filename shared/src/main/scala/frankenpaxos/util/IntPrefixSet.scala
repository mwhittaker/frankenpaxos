package frankenpaxos.util

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object IntPrefixSet {
  @JSExport("apply")
  def apply(): IntPrefixSet = new IntPrefixSet(Set())
}

// An IntPrefixSetis an add-only set of natural numbers (i.e., integers greater
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
@JSExportAll
class IntPrefixSet private (initialValues: Set[Int])
    extends CompactSet[IntPrefixSet] {
  override type T = Int

  private var watermark: Int = 0
  private var values: mutable.Set[Int] = mutable.Set() ++ initialValues
  compact()

  override def toString(): String =
    s"IntPrefixSet(< $watermark, $values)"

  override def add(x: Int): Boolean = {
    require(x >= 0)

    if (x < watermark) {
      return false
    }

    val freshlyInserted = values.add(x)
    compact()
    freshlyInserted
  }

  override def contains(x: Int): Boolean = {
    require(x >= 0)
    x < watermark || values.contains(x)
  }

  override def diff(other: IntPrefixSet): IntPrefixSet = {
    new IntPrefixSet(
      values.toSet ++ (other.watermark until watermark) -- other.values.toSet
    )
  }

  override def materialize(): Set[Int] = values.toSet ++ (0 until watermark)

  def getWatermark(): Int = watermark

  def +(x: Int): IntPrefixSet = {
    add(x)
    this
  }

  // Compact `values`, making it as small as possible and making `watermark` as
  // large as possible.
  private def compact(): Unit = {
    while (values.contains(watermark)) {
      values.remove(watermark)
      watermark += 1
    }
  }
}
