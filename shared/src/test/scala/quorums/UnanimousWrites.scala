package frankenpaxos.quorums

import com.google.protobuf.ByteString
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable

class UnanimousWritesTest extends FlatSpec with Matchers {
  "A simple majority quorum system" should "compute read quorums correctly" in {
    val qs = new UnanimousWrites(Set(0, 1, 2, 3, 4))
    qs.isReadQuorum(Set()) shouldBe false
    qs.isReadQuorum(Set(0)) shouldBe true
    qs.isReadQuorum(Set(1)) shouldBe true
    qs.isReadQuorum(Set(2)) shouldBe true
    qs.isReadQuorum(Set(3)) shouldBe true
    qs.isReadQuorum(Set(4)) shouldBe true
    qs.isReadQuorum(Set(0, 1)) shouldBe true
    qs.isReadQuorum(Set(0, 1, 2)) shouldBe true
    qs.isReadQuorum(Set(0, 1, 2, 3)) shouldBe true
    qs.isReadQuorum(Set(0, 1, 2, 3, 4)) shouldBe true
  }

  it should "compute write quorums correctly" in {
    val qs = new UnanimousWrites(Set(0, 1, 2, 3, 4))
    qs.isWriteQuorum(Set()) shouldBe false
    qs.isWriteQuorum(Set(0)) shouldBe false
    qs.isWriteQuorum(Set(0, 1)) shouldBe false
    qs.isWriteQuorum(Set(0, 1, 2)) shouldBe false
    qs.isWriteQuorum(Set(0, 1, 2, 3)) shouldBe false
    qs.isWriteQuorum(Set(0, 1, 2, 3, 4)) shouldBe true
  }

  it should "compute read quorum supersets correctly" in {
    val qs = new UnanimousWrites(Set(0, 1, 2, 3, 4))
    qs.isSuperSetOfReadQuorum(Set()) shouldBe false
    qs.isSuperSetOfReadQuorum(Set(0)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(1)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(2)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(3)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(4)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1, 2)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1, 2, 3)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1, 2, 3, 4)) shouldBe true

    qs.isSuperSetOfReadQuorum(Set(5)) shouldBe false
    qs.isSuperSetOfReadQuorum(Set(0, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(1, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(2, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(3, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(4, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1, 2, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1, 2, 3, 5)) shouldBe true
    qs.isSuperSetOfReadQuorum(Set(0, 1, 2, 3, 4, 5)) shouldBe true
  }

  it should "compute write quorum supersets correctly" in {
    val qs = new UnanimousWrites(Set(0, 1, 2, 3, 4))
    qs.isSuperSetOfWriteQuorum(Set()) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1, 2)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1, 2, 3)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1, 2, 3, 4)) shouldBe true

    qs.isSuperSetOfWriteQuorum(Set(5)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 5)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1, 5)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1, 2, 5)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1, 2, 3, 5)) shouldBe false
    qs.isSuperSetOfWriteQuorum(Set(0, 1, 2, 3, 4, 5)) shouldBe true
  }
}
