package frankenpaxos.quorums

import com.google.protobuf.ByteString
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable

class SimpleMajorityTest extends FlatSpec with Matchers {
  "A simple majority quorum system" should "compute read quorums correctly" in {
    val qs = new SimpleMajority(Set(0, 1, 2, 3, 4))
    qs.isReadQuorum(Set()) shouldBe false
    qs.isReadQuorum(Set(0)) shouldBe false
    qs.isReadQuorum(Set(0, 1)) shouldBe false
    qs.isReadQuorum(Set(0, 1, 2)) shouldBe true
    qs.isReadQuorum(Set(0, 1, 2, 3)) shouldBe true
    qs.isReadQuorum(Set(0, 1, 2, 3, 4)) shouldBe true
  }

  it should "compute write quorums correctly" in {
    val qs = new SimpleMajority(Set(0, 1, 2, 3, 4))
    qs.isWriteQuorum(Set()) shouldBe false
    qs.isWriteQuorum(Set(0)) shouldBe false
    qs.isWriteQuorum(Set(0, 1)) shouldBe false
    qs.isWriteQuorum(Set(0, 1, 2)) shouldBe true
    qs.isWriteQuorum(Set(0, 1, 2, 3)) shouldBe true
    qs.isWriteQuorum(Set(0, 1, 2, 3, 4)) shouldBe true
  }
}
