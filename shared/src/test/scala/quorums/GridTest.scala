package frankenpaxos.quorums

import com.google.protobuf.ByteString
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable

class GridTest extends FlatSpec with Matchers {
  "A grid quorum system" should "compute read quorums correctly" in {
    val qs = new Grid(Seq(Seq(1, 2, 3), Seq(4, 5, 6)))
    qs.isReadQuorum(Set()) shouldBe false
    for (i <- 1 to 6) {
      qs.isReadQuorum(Set(i)) shouldBe false
    }
    for (i <- 1 to 6; j <- 1 to 6) {
      qs.isReadQuorum(Set(i, j)) shouldBe false
    }
    qs.isReadQuorum(Set(1, 2, 4)) shouldBe false
    qs.isReadQuorum(Set(4, 5, 3)) shouldBe false
    qs.isReadQuorum(Set(1, 2, 3)) shouldBe true
    qs.isReadQuorum(Set(4, 5, 6)) shouldBe true
    qs.isReadQuorum(Set(1, 2, 3, 4)) shouldBe true
    qs.isReadQuorum(Set(1, 2, 3, 4, 5)) shouldBe true
    qs.isReadQuorum(Set(1, 2, 3, 4, 5, 6)) shouldBe true
  }

  it should "compute write quorums correctly" in {
    val qs = new Grid(Seq(Seq(1, 2, 3), Seq(4, 5, 6)))
    qs.isWriteQuorum(Set()) shouldBe false
    for (i <- 1 to 6) {
      qs.isWriteQuorum(Set(i)) shouldBe false
    }
    qs.isWriteQuorum(Set(1, 2)) shouldBe false
    qs.isWriteQuorum(Set(1, 2, 3)) shouldBe false
    qs.isWriteQuorum(Set(4, 5)) shouldBe false
    qs.isWriteQuorum(Set(4, 5, 6)) shouldBe false
    qs.isWriteQuorum(Set(1, 4)) shouldBe true
    qs.isWriteQuorum(Set(2, 4)) shouldBe true
    qs.isWriteQuorum(Set(2, 5)) shouldBe true
    qs.isWriteQuorum(Set(1, 2, 4)) shouldBe true
    qs.isWriteQuorum(Set(1, 2, 4, 5)) shouldBe true
    qs.isWriteQuorum(Set(1, 2, 3, 4, 5)) shouldBe true
    qs.isWriteQuorum(Set(1, 2, 3, 4, 5, 6)) shouldBe true
  }

  it should "compute read quorum supersets correctly" in {
    val qs = new Grid(Seq(Seq(1, 2, 3), Seq(4, 5, 6)))
    qs.isSuperSetOfReadQuorum(Set()) shouldBe false
    for (i <- 1 to 6) {
      qs.isSuperSetOfReadQuorum(Set(i)) shouldBe false
    }
    for (i <- 1 to 6; j <- 1 to 6) {
      qs.isSuperSetOfReadQuorum(Set(i, j)) shouldBe false
    }
    qs.isSuperSetOfReadQuorum(Set(1, 2, 4)) shouldBe false
    qs.isSuperSetOfReadQuorum(Set(4, 5, 3)) shouldBe false
    qs.isSuperSetOfReadQuorum(Set(1, 2, 3)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(4, 5, 6)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(1, 2, 3, 4)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(1, 2, 3, 4, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(1, 2, 3, 4, 5, 6)) shouldBe true

    qs.isSuperSetOfReadQuorum(Set(9001, 1, 2, 4)) shouldBe false
    qs.isSuperSetOfReadQuorum(Set(9001, 4, 5, 3)) shouldBe false
    qs.isSuperSetOfReadQuorum(Set(9001, 1, 2, 3)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(9001, 4, 5, 6)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(9001, 1, 2, 3, 4)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(9001, 1, 2, 3, 4, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(9001, 1, 2, 3, 4, 5, 6)) shouldBe true
  }

  it should "compute write quorum supersets correctly" in {
    val qs = new Grid(Seq(Seq(1, 2, 3), Seq(4, 5, 6)))
    qs.isSuperSetOfWriteQuorum(Set()) shouldBe false
    for (i <- 1 to 6) {
      qs.isSuperSetOfWriteQuorum(Set(i)) shouldBe false
    }
    qs.isSuperSetOfWriteQuorum(Set(1, 2)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(1, 2, 3)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(4, 5)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(4, 5, 6)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(1, 4)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(2, 4)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(2, 5)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(1, 2, 4)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(1, 2, 4, 5)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(1, 2, 3, 4, 5)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(1, 2, 3, 4, 5, 6)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(9001, 1, 2)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(9001, 1, 2, 3)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(9001, 4, 5)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(9001, 4, 5, 6)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(9001, 1, 4)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(9001, 2, 4)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(9001, 2, 5)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(9001, 1, 2, 4)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(9001, 1, 2, 4, 5)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(9001, 1, 2, 3, 4, 5)) shouldBe true
    qs.isSuperSetOfWriteQuorum(Set(9001, 1, 2, 3, 4, 5, 6)) shouldBe true
  }
}
