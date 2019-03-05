package frankenpaxos

import collection.mutable
import org.scalatest._

class UtilSpec extends FlatSpec {
  "histogram" should "work correctly" in {
    assertResult(mutable.Map())(Util.histogram(Seq()))
    assertResult(mutable.Map(1 -> 1, 2 -> 1, 3 -> 1, 4 -> 1)) {
      Util.histogram(Seq(1, 2, 3, 4))
    }
    assertResult(mutable.Map(1 -> 1, 2 -> 1, 3 -> 1, 4 -> 2)) {
      Util.histogram(Seq(1, 2, 4, 3, 4))
    }
    assertResult(mutable.Map(1 -> 2, 2 -> 1, 3 -> 1, 4 -> 2)) {
      Util.histogram(Seq(4, 1, 2, 1, 3, 4))
    }
    assertResult(mutable.Map(1 -> 2, 2 -> 1, 3 -> 1, 4 -> 3)) {
      Util.histogram(Seq(4, 1, 2, 1, 4, 3, 4))
    }
  }

  "popularItems" should "work correctly" in {
    assertResult(Set())(Util.popularItems(Seq(), 42))
    assertResult(Set())(Util.popularItems(Seq(1, 2, 3, 4), 2))
    assertResult(Set(4))(Util.popularItems(Seq(1, 2, 4, 3, 4), 2))
    assertResult(Set(1, 4))(Util.popularItems(Seq(4, 1, 2, 1, 3, 4), 2))
    assertResult(Set(1, 4))(Util.popularItems(Seq(4, 1, 2, 1, 4, 3, 4), 2))
  }
}
