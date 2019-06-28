package frankenpaxos

import collection.mutable
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class UtilSpec extends FlatSpec with Matchers {
  "histogram" should "work correctly" in {
    Util.histogram(Seq()) shouldBe mutable.Map()
    Util.histogram(Seq(1, 2, 3, 4)) shouldBe
      mutable.Map(1 -> 1, 2 -> 1, 3 -> 1, 4 -> 1)
    Util.histogram(Seq(1, 2, 4, 3, 4)) shouldBe
      mutable.Map(1 -> 1, 2 -> 1, 3 -> 1, 4 -> 2)
    Util.histogram(Seq(4, 1, 2, 1, 3, 4)) shouldBe
      mutable.Map(1 -> 2, 2 -> 1, 3 -> 1, 4 -> 2)
    Util.histogram(Seq(4, 1, 2, 1, 4, 3, 4)) shouldBe
      mutable.Map(1 -> 2, 2 -> 1, 3 -> 1, 4 -> 3)
  }

  "popularItems" should "work correctly" in {
    Util.popularItems(Seq(), 42) shouldBe Set()
    Util.popularItems(Seq(1, 2, 3, 4), 2) shouldBe Set()
    Util.popularItems(Seq(1, 2, 4, 3, 4), 2) shouldBe Set(4)
    Util.popularItems(Seq(4, 1, 2, 1, 3, 4), 2) shouldBe Set(1, 4)
    Util.popularItems(Seq(4, 1, 2, 1, 4, 3, 4), 2) shouldBe Set(1, 4)
  }

  "merge" should "work correctly" in {
    import Util._

    val left = Map[Int, Int](1 -> 1, 2 -> 2)
    val right = Map[Int, String](2 -> "two", 3 -> "three")
    val merged = left.merge(right) {
      case (_, Left(l))    => l.toString()
      case (_, Both(l, r)) => l.toString() + r
      case (_, Right(r))   => r
    }
    merged shouldBe Map(1 -> "1", 2 -> "2two", 3 -> "three")
  }
}
