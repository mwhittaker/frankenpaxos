package frankenpaxos.simplebpaxos

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class QuorumWatermarkTest extends FlatSpec with Matchers {
  "A QuorumWatermark" should "work correctly" in {
    val watermark = new QuorumWatermark(numWatermarks = 3)
    watermark.watermark(3) shouldBe 0
    watermark.watermark(2) shouldBe 0
    watermark.watermark(1) shouldBe 0

    watermark.update(0, 1)
    watermark.update(1, 2)
    watermark.update(2, 3)
    watermark.watermark(3) shouldBe 1
    watermark.watermark(2) shouldBe 2
    watermark.watermark(1) shouldBe 3

    watermark.update(0, 0) // 1
    watermark.update(1, 4) // 4
    watermark.update(2, 6) // 6
    watermark.watermark(3) shouldBe 1
    watermark.watermark(2) shouldBe 4
    watermark.watermark(1) shouldBe 6

    watermark.update(0, 7) // 7
    watermark.update(1, 2) // 4
    watermark.update(2, 8) // 8
    watermark.watermark(3) shouldBe 4
    watermark.watermark(2) shouldBe 7
    watermark.watermark(1) shouldBe 8

    watermark.update(0, 9) // 9
    watermark.update(1, 5) // 5
    watermark.update(2, 7) // 8
    watermark.watermark(3) shouldBe 5
    watermark.watermark(2) shouldBe 8
    watermark.watermark(1) shouldBe 9
  }
}
