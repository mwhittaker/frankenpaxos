package frankenpaxos.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class QuorumWatermarkVectorTest extends FlatSpec with Matchers {
  "A QuorumWatermarkVector" should "work correctly with quorumSize 4" in {
    val watermark = new QuorumWatermarkVector(n = 4, depth = 3)
    // [0, 0, 0]
    // [0, 0, 0]
    // [0, 0, 0]
    // [0, 0, 0]
    watermark.watermark(4) shouldBe Seq(0, 0, 0)
    watermark.watermark(3) shouldBe Seq(0, 0, 0)
    watermark.watermark(2) shouldBe Seq(0, 0, 0)
    watermark.watermark(1) shouldBe Seq(0, 0, 0)

    watermark.update(0, Seq(1, 2, 3)) // [1, 2, 3]
    watermark.update(1, Seq(6, 5, 4)) // [6, 5, 4]
    watermark.update(2, Seq(2, 4, 6)) // [2, 4, 6]
    watermark.update(3, Seq(7, 5, 3)) // [7, 5, 3]
    watermark.watermark(4) shouldBe Seq(1, 2, 3)
    watermark.watermark(3) shouldBe Seq(2, 4, 3)
    watermark.watermark(2) shouldBe Seq(6, 5, 4)
    watermark.watermark(1) shouldBe Seq(7, 5, 6)

    watermark.update(0, Seq(3, 8, 2)) // [3, 8, 3]
    watermark.update(1, Seq(6, 4, 3)) // [6, 5, 4]
    watermark.update(2, Seq(7, 5, 9)) // [7, 5, 9]
    watermark.update(3, Seq(4, 9, 8)) // [7, 9, 8]
    watermark.watermark(4) shouldBe Seq(3, 5, 3)
    watermark.watermark(3) shouldBe Seq(6, 5, 4)
    watermark.watermark(2) shouldBe Seq(7, 8, 8)
    watermark.watermark(1) shouldBe Seq(7, 9, 9)

    watermark.update(0, Seq(9, 5, 8)) // [9, 8, 8]
    watermark.update(1, Seq(4, 3, 6)) // [6, 5, 6]
    watermark.update(2, Seq(4, 6, 9)) // [7, 6, 9]
    watermark.update(3, Seq(8, 1, 3)) // [8, 9, 8]
    watermark.watermark(4) shouldBe Seq(6, 5, 6)
    watermark.watermark(3) shouldBe Seq(7, 6, 8)
    watermark.watermark(2) shouldBe Seq(8, 8, 8)
    watermark.watermark(1) shouldBe Seq(9, 9, 9)
  }
}
