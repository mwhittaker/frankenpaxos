package frankenpaxos.compact

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object IntPrefixSet {
  // Construct an empty IntPrefixSet.
  @JSExport("apply")
  def apply(): IntPrefixSet = new IntPrefixSet(0, mutable.Set())

  // Construct an IntPrefixSet from a standard, uncompacted set.
  @JSExport("apply")
  def apply(values: Set[Int]): IntPrefixSet =
    new IntPrefixSet(0, values.to[mutable.Set])

  // Construct an IntPrefixSet from an IntPrefixSetProto. It is a precondition
  // that `proto` was generated using `IntPrefixSet.toProto`. You cannot pass
  // in an arbitrary IntPrefixSetProto.
  def fromProto(proto: IntPrefixSetProto): IntPrefixSet = {
    new IntPrefixSet(proto.watermark, proto.value.to[mutable.Set])
  }
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
class IntPrefixSet private (
    private var watermark: Int,
    private val values: mutable.Set[Int]
) extends CompactSet[IntPrefixSet] {
  override type T = Int

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

  override def union(other: IntPrefixSet): IntPrefixSet = {
    val maxWatermark = Math.max(watermark, other.watermark)
    new IntPrefixSet(
      maxWatermark,
      (values ++ other.values).filter(_ >= maxWatermark)
    )
  }

  override def diff(other: IntPrefixSet): IntPrefixSet = {
    new IntPrefixSet(
      0,
      values ++ (other.watermark until watermark) -- other.values
    )
  }

  override def size: Int = watermark + values.size

  override def uncompactedSize: Int = values.size

  override def materialize(): Set[Int] = values.toSet ++ (0 until watermark)

  def getWatermark(): Int = watermark

  def +(x: Int): IntPrefixSet = {
    add(x)
    this
  }

  def toProto(): IntPrefixSetProto =
    IntPrefixSetProto(watermark = watermark, value = values.toSeq)

  // Compact `values`, making it as small as possible and making `watermark` as
  // large as possible.
  private def compact(): Unit = {
    while (values.contains(watermark)) {
      values.remove(watermark)
      watermark += 1
    }
  }
}
