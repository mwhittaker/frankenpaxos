package frankenpaxos.scalog

import collection.mutable
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

class AggregatorTest extends FlatSpec with Matchers with PropertyChecks {
  "Aggregator.findSlot" should "find with one server correctly" in {
    //   0   1   2   3   4   5   6   7   8   9
    // +---+---+---+---+---+---+---+---+---+---+
    // |   |   |   |   |   |   |   |   |   |   |
    // +---+---+---+---+---+---+---+---+---+---+
    //  \_/ \_____/ \_____/
    // 0 1     2       3
    val cuts = mutable.Buffer[Aggregator.Cut](Seq(0), Seq(1), Seq(3), Seq(5))

    Aggregator.findSlot(cuts, 0) shouldBe Some((1, 0))
    Aggregator.findSlot(cuts, 1) shouldBe Some((2, 0))
    Aggregator.findSlot(cuts, 2) shouldBe Some((2, 0))
    Aggregator.findSlot(cuts, 3) shouldBe Some((3, 0))
    Aggregator.findSlot(cuts, 4) shouldBe Some((3, 0))
    Aggregator.findSlot(cuts, 5) shouldBe None
  }

  it should "find with two servers correctly" in {
    //   0   1   2   3   4   5   6   7   8   9
    // +---+---+---+---+---+---+---+---+---+---+
    // | 0 | 1 | 1 | 0 | 1 | 1 |   |   |   |   |
    // +---+---+---+---+---+---+---+---+---+---+
    //  \_/ \_/ \_/ \_________/
    // 0 1   2   3       4
    val cuts = mutable.Buffer[Aggregator.Cut](
      Seq(0, 0),
      Seq(1, 0),
      Seq(1, 1),
      Seq(1, 2),
      Seq(2, 4)
    )

    Aggregator.findSlot(cuts, 0) shouldBe Some((1, 0))
    Aggregator.findSlot(cuts, 1) shouldBe Some((2, 1))
    Aggregator.findSlot(cuts, 2) shouldBe Some((3, 1))
    Aggregator.findSlot(cuts, 3) shouldBe Some((4, 0))
    Aggregator.findSlot(cuts, 4) shouldBe Some((4, 1))
    Aggregator.findSlot(cuts, 5) shouldBe Some((4, 1))
    Aggregator.findSlot(cuts, 6) shouldBe None
  }

  it should "find with four servers correctly" in {
    //   0   1   2   3   4   5   6   7   8   9
    // +---+---+---+---+---+---+---+---+---+---+
    // | 0 | 1 | 1 | 3 | 0 | 2 |   |   |   |   |
    // +---+---+---+---+---+---+---+---+---+---+
    //  \_/ \_________/ \_____/
    // 0 1       2         3
    val cuts = mutable.Buffer[Aggregator.Cut](
      Seq(0, 0, 0, 0),
      Seq(1, 0, 0, 0),
      Seq(1, 2, 0, 1),
      Seq(2, 2, 1, 1)
    )

    Aggregator.findSlot(cuts, 0) shouldBe Some((1, 0))
    Aggregator.findSlot(cuts, 1) shouldBe Some((2, 1))
    Aggregator.findSlot(cuts, 2) shouldBe Some((2, 1))
    Aggregator.findSlot(cuts, 3) shouldBe Some((2, 3))
    Aggregator.findSlot(cuts, 4) shouldBe Some((3, 0))
    Aggregator.findSlot(cuts, 5) shouldBe Some((3, 2))
    Aggregator.findSlot(cuts, 6) shouldBe None
  }
}
