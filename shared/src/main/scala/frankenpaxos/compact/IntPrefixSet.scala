package frankenpaxos.compact

import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object IntPrefixSet {
  // Construct an empty IntPrefixSet.
  @JSExport("apply")
  def apply(): IntPrefixSet = new IntPrefixSet(0, mutable.Set())

  // Construct an IntPrefixSet from a watermark.
  def fromWatermark(watermark: Int): IntPrefixSet =
    new IntPrefixSet(watermark, mutable.Set[Int]())

  // Construct an IntPrefixSet from a standard, uncompacted set.
  @JSExport("apply")
  def apply(values: Set[Int]): IntPrefixSet =
    new IntPrefixSet(0, values.to[mutable.Set])

  // Construct an IntPrefixSet from a standard, uncompacted set.
  @JSExport("apply")
  def fromMutableSet(values: mutable.Set[Int]): IntPrefixSet =
    new IntPrefixSet(0, values)

  // Construct an IntPrefixSet from a watermark and set. It is assumed, but not
  // enforced, that every value in values is greater than or equal to watermark.
  @JSExport("apply")
  def apply(watermark: Int, values: Set[Int]): IntPrefixSet =
    new IntPrefixSet(watermark, values.to[mutable.Set])

  // Construct an IntPrefixSet from a watermark and set. It is assumed, but not
  // enforced, that every value in values is greater than or equal to watermark.
  def fromWatermarkAndMutableSet(
      watermark: Int,
      values: mutable.Set[Int]
  ): IntPrefixSet =
    new IntPrefixSet(watermark, values)

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

  class DiffIterator(me: IntPrefixSet, other: IntPrefixSet)
      extends Iterator[Int] {
    val iterator: Iterator[Int] = if (me.values.isEmpty) {
      new WatermarkIterator(0, me.watermark, other)
    } else {
      new WatermarkIterator(0, me.watermark, other) ++
        new ValuesIterator(me.values.iterator, other)
    }

    override def hasNext: Boolean = iterator.hasNext
    override def next(): Int = iterator.next()
  }

  trait OptionIterator[A] extends Iterator[A] {
    def getNext(): Option[A]

    private var cached: Option[Option[A]] = None

    override def hasNext(): Boolean = {
      cached match {
        case None =>
          val x = getNext()
          cached = Some(x)
          x.isDefined
        case Some(x) =>
          x.isDefined
      }
    }

    override def next(): A = {
      cached match {
        case None =>
          getNext().get
        case Some(x) =>
          cached = None
          x.get
      }
    }
  }

  class ValuesIterator(values: Iterator[Int], other: IntPrefixSet)
      extends OptionIterator[Int] {
    override def getNext(): Option[Int] = {
      if (!values.hasNext) {
        return None
      }

      var x = values.next()
      while (x < other.watermark || other.values.contains(x)) {
        if (!values.hasNext) {
          return None
        }
        x = values.next()
      }

      Some(x)
    }
  }

  class WatermarkIterator(from: Int, to: Int, other: IntPrefixSet)
      extends OptionIterator[Int] {
    private var x: Int = from

    override def getNext(): Option[Int] = {
      WatermarkIterator.getNext(x, to, other) match {
        case None =>
          None
        case Some(y) =>
          x = y + 1
          Some(y)
      }
    }
  }

  object WatermarkIterator {
    def getNext(from: Int, to: Int, other: IntPrefixSet): Option[Int] = {
      // If `from` (inclusive) is greater than or equal to `to` (exclusive),
      // then our range is empty.
      if (from >= to) {
        return None
      }

      // If `to` is less than or equal to `other.watermark`, then our entire
      // range is covered by other's watermark, so we don't have any values to
      // return.
      //
      //          |
      //          |-- watermark
      //     to --|
      //          |
      //          |
      //   from --|
      if (to <= other.watermark) {
        return None
      }

      // Otherwise, `to` is larger than the other watermark. There are two
      // cases to consider. First, if `from` is larger than or equal to
      // watermark:
      //
      //     to --|
      //          |
      //          |
      //   from --|
      //          |-- watermark
      //          |
      //
      // then we begin our iteration from `from`. Second, if `from` is smaller
      // than watermark, then we begin our iteration from watermark.
      //
      //     to --|
      //          |
      //          |
      //          |-- watermark
      //          |
      //   from --|
      var startIterationFrom = Math.max(from, other.watermark)

      // Next, we simply find the first value greater than or equal to
      // `startIterationFrom` that is not in `other.values`. As a special case,
      // if `other.values` is empty, we can return immediately.
      if (other.values.isEmpty) {
        return Some(startIterationFrom)
      }

      while (other.values.contains(startIterationFrom)) {
        startIterationFrom += 1
        if (startIterationFrom >= to) {
          return None
        }
      }
      Some(startIterationFrom)
    }
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
    private var values: mutable.Set[Int]
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

  override def clone(): IntPrefixSet =
    new IntPrefixSet(watermark, values.clone())

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
      // TODO(mwhittaker): We have to clone values.
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

  override def materializedDiff(other: IntPrefixSet): Iterable[Int] = {
    if (watermark <= other.watermark) {
      if (values.isEmpty) {
        Seq()
      } else if (other.values.isEmpty) {
        values.view.filter(_ >= other.watermark)
      } else {
        values.view.filter(
          x => x >= other.watermark && !other.values.contains(x)
        )
      }
    } else {
      if (other.values.isEmpty && values.isEmpty) {
        (other.watermark until watermark).view
      } else if (other.values.isEmpty) {
        (other.watermark until watermark).view ++ values.view
      } else if (values.isEmpty) {
        (other.watermark until watermark).view
          .filter(!other.values.contains(_))
      } else {
        ((other.watermark until watermark).view ++ values.view)
          .filter(!other.values(_))
      }
    }
  }

  override def diffIterator(other: IntPrefixSet): Iterator[Int] =
    new IntPrefixSet.DiffIterator(this, other)

  override def addAll(other: IntPrefixSet): this.type = {
    // Through benchmarking, checking if various sets are empty actually speeds
    // things up. I know, it's suprising.
    if (values.isEmpty && other.values.isEmpty) {
      watermark = Math.max(watermark, other.watermark)
    } else if (!values.isEmpty && other.values.isEmpty) {
      if (watermark >= other.watermark) {
        // Do nothing.
      } else {
        watermark = other.watermark
        values.retain(_ >= watermark)
        compact()
      }
    } else if (values.isEmpty && !other.values.isEmpty) {
      values = other.values.clone()
      if (other.watermark >= watermark) {
        watermark = other.watermark
      } else {
        values.retain(_ >= watermark)
        compact()
      }
    } else if (!values.isEmpty && !other.values.isEmpty) {
      if (watermark >= other.watermark) {
        values ++= other.values.iterator.filter(_ >= watermark)
        compact()
      } else {
        watermark = other.watermark
        values.retain(_ >= other.watermark)
        values ++= other.values
        compact()
      }
    }

    this
  }

  override def subtractAll(other: IntPrefixSet): this.type = {
    if ((watermark == 0 && values.isEmpty) ||
        (other.watermark == 0 && other.values.isEmpty)) {
      return this
    }

    if (other.watermark == 0) {
      val minOtherValue = other.values.min
      if (minOtherValue >= watermark) {
        values --= other.values
      } else {
        for (i <- minOtherValue + 1 until watermark) {
          values += i
        }
        values --= other.values
        watermark = minOtherValue
      }
    } else if (watermark == 0) {
      values.retain(x => x >= other.watermark)
      values --= other.values
    } else if (other.watermark <= watermark) {
      for (i <- other.watermark until watermark) {
        values += i
      }
      values --= other.values
      watermark = 0
    } else {
      values.retain(_ >= other.watermark)
      values --= other.values
      watermark = 0
    }

    this
  }

  override def subtractOne(x: Int): this.type = {
    if (x >= watermark) {
      values -= x
    } else {
      for (i <- x + 1 until watermark) {
        values += i
      }
      watermark = x
    }
    this
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

  def toProto(): IntPrefixSetProto = {
    if (values.size == 0) {
      IntPrefixSetProto(watermark = watermark, value = Seq())
    } else {
      IntPrefixSetProto(watermark = watermark, value = values.toSeq)
    }
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
