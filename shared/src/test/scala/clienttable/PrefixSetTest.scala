package frankenpaxos.clienttable

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class PrefixSetSpec extends FlatSpec with Matchers {
  "A prefix set" should "insert 0 correctly" in {
    val prefixSet = new PrefixSet()
    prefixSet.add(0)
    prefixSet.contains(0) shouldBe true
    prefixSet.contains(1) shouldBe false
    prefixSet.contains(2) shouldBe false
  }

  it should "insert ascending chain correctly" in {
    val prefixSet = new PrefixSet()
    prefixSet.add(0)
    prefixSet.add(1)
    prefixSet.add(2)
    prefixSet.contains(0) shouldBe true
    prefixSet.contains(1) shouldBe true
    prefixSet.contains(2) shouldBe true
    prefixSet.contains(3) shouldBe false
    prefixSet.contains(4) shouldBe false
  }

  it should "insert contiguous chain in random order correctly" in {
    val prefixSet = new PrefixSet()
    prefixSet.add(4)
    prefixSet.add(0)
    prefixSet.add(2)
    prefixSet.add(1)
    prefixSet.add(3)
    prefixSet.contains(0) shouldBe true
    prefixSet.contains(1) shouldBe true
    prefixSet.contains(2) shouldBe true
    prefixSet.contains(3) shouldBe true
    prefixSet.contains(4) shouldBe true
    prefixSet.contains(5) shouldBe false
  }

  it should "insert disjoint set in random order correctly" in {
    val prefixSet = new PrefixSet()
    prefixSet.add(20)
    prefixSet.add(7)
    prefixSet.add(0)
    prefixSet.add(3)
    prefixSet.add(2)
    prefixSet.add(30)
    prefixSet.add(6)
    prefixSet.add(1)
    prefixSet.add(4)
    prefixSet.add(10)

    prefixSet.contains(0) shouldBe true
    prefixSet.contains(1) shouldBe true
    prefixSet.contains(2) shouldBe true
    prefixSet.contains(3) shouldBe true
    prefixSet.contains(4) shouldBe true
    prefixSet.contains(5) shouldBe false
    prefixSet.contains(6) shouldBe true
    prefixSet.contains(7) shouldBe true
    prefixSet.contains(8) shouldBe false
    prefixSet.contains(9) shouldBe false
    prefixSet.contains(10) shouldBe true
    prefixSet.contains(20) shouldBe true
    prefixSet.contains(30) shouldBe true
  }
}
