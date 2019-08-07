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

  // Construct an IntPrefixSet from a watermark and set.
  @JSExport("apply")
  def apply(watermark: Int, values: Set[Int]): IntPrefixSet = {
    require(watermark >= 0)
    require(values.forall(_ >= watermark))
    new IntPrefixSet(watermark, values.to[mutable.Set])
  }

  // Construct an IntPrefixSet from an IntPrefixSetProto. It is a precondition
  // that `proto` was generated using `IntPrefixSet.toProto`. You cannot pass
  // in an arbitrary IntPrefixSetProto.
  def fromProto(proto: IntPrefixSetProto): IntPrefixSet = {
    new IntPrefixSet(proto.watermark, proto.value.to[mutable.Set])
  }

  implicit val factory = new CompactSetFactory[IntPrefixSet, Int] {
    override def empty = IntPrefixSet()
    override def fromSet(xs: Set[Int]) = IntPrefixSet(xs)
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

  private def toTuple(): (Int, mutable.Set[Int]) = (watermark, values)

  override def equals(other: Any): Boolean = {
    other match {
      case other: IntPrefixSet => toTuple() == other.toTuple()
      case _                   => false
    }
  }

  override def hashCode: Int = toTuple().hashCode

  override def toString(): String = {
    if (values.isEmpty) {
      s"{<$watermark}"
    } else {
      s"{<$watermark, ${values.mkString(",")}}"
    }
  }

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
    if (other.watermark == 0 && other.values.isEmpty) {
      new IntPrefixSet(watermark, values)
    } else if (other.watermark == 0) {
      val minOtherValue = other.values.min
      if (minOtherValue >= watermark) {
        new IntPrefixSet(watermark, values -- other.values)
      } else {
        new IntPrefixSet(
          minOtherValue,
          values ++ (minOtherValue until watermark) -- other.values
        )
      }
    } else if (other.watermark <= watermark) {
      new IntPrefixSet(
        0,
        values ++ (other.watermark until watermark) -- other.values
      )
    } else {
      new IntPrefixSet(
        0,
        values.filter(_ >= other.watermark) -- other.values
      )
    }
  }

  override def size: Int = watermark + values.size

  override def uncompactedSize: Int = values.size

  override def subset(): IntPrefixSet =
    new IntPrefixSet(watermark, mutable.Set())

  def getWatermark(): Int = watermark

  override def materialize(): Set[Int] = values.toSet ++ (0 until watermark)

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
