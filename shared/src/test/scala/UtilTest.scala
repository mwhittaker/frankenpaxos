package frankenpaxos

import org.scalatest._

class UtilSpec extends FlatSpec {
  "popularItems" should "work correctly" in {
    assertResult(Set())(Util.popularItems(Seq(), 42))
    assertResult(Set())(Util.popularItems(Seq(1, 2, 3, 4), 2))
    assertResult(Set(4))(Util.popularItems(Seq(1, 2, 4, 3, 4), 2))
    assertResult(Set(1, 4))(Util.popularItems(Seq(4, 1, 2, 1, 3, 4), 2))
    assertResult(Set(1, 4))(Util.popularItems(Seq(4, 1, 2, 1, 4, 3, 4), 2))
  }
}
