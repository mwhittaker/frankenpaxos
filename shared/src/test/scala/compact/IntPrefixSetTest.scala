package frankenpaxos.compact

import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

class IntPrefixSetTest extends FlatSpec with Matchers with PropertyChecks {
  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 2500)

  case class OneParams(set: Set[Int])
  case class TwoParams(lhs: Set[Int], rhs: Set[Int])

  val oneGen = for {
    watermark <- Gen.chooseNum(0, 100)
    values <- Gen.containerOf[Set, Int](Gen.choose(0, 100))
  } yield OneParams((0 until watermark).toSet ++ values)

  val twoGen = for {
    lhs <- oneGen
    rhs <- oneGen
  } yield TwoParams(lhs.set, rhs.set)

  "An IntPrefixSet" should "add 0 correctly" in {
    val prefixSet = IntPrefixSet()
    prefixSet.add(0)
    prefixSet.contains(0) shouldBe true
    prefixSet.contains(1) shouldBe false
    prefixSet.contains(2) shouldBe false
    prefixSet.materialize() shouldBe Set(0)
    prefixSet.getWatermark() shouldBe 1
  }

  it should "add ascending chain correctly" in {
    val prefixSet = IntPrefixSet()
    prefixSet.add(0)
    prefixSet.add(1)
    prefixSet.add(2)
    prefixSet.contains(0) shouldBe true
    prefixSet.contains(1) shouldBe true
    prefixSet.contains(2) shouldBe true
    prefixSet.contains(3) shouldBe false
    prefixSet.contains(4) shouldBe false
    prefixSet.materialize() shouldBe Set(0, 1, 2)
    prefixSet.getWatermark() shouldBe 3
  }

  it should "add contiguous chain in random order correctly" in {
    val prefixSet = IntPrefixSet()
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
    prefixSet.materialize() shouldBe Set(0, 1, 2, 3, 4)
    prefixSet.getWatermark() shouldBe 5
  }

  it should "add disjoint set in random order correctly" in {
    val prefixSet = IntPrefixSet()
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
    prefixSet.materialize() shouldBe Set(0, 1, 2, 3, 4, 6, 7, 10, 20, 30)
    prefixSet.getWatermark() shouldBe 5
  }

  it should "construct from a set correctly" in {
    val gen = Gen.containerOf[Set, Int](Gen.choose(0, 1000000))
    forAll(gen) { (xs: Set[Int]) =>
      val prefixSet = IntPrefixSet(xs)
      prefixSet.materialize() shouldBe xs
      xs.forall(prefixSet.contains) shouldBe true
    }
  }

  it should "union disjoint sets correctly" in {
    val lhs = IntPrefixSet(Set(0, 1, 2))
    val rhs = IntPrefixSet(Set(3, 4, 5))
    val union = lhs.union(rhs)
    union shouldBe IntPrefixSet(Set(0, 1, 2, 3, 4, 5))
    union.getWatermark() shouldBe 6
  }

  it should "union left set with larger watermark correctly" in {
    val lhs = IntPrefixSet(Set(0, 1, 2, 10, 20))
    val rhs = IntPrefixSet(Set(0, 2, 10, 30))
    val union = lhs.union(rhs)
    union shouldBe IntPrefixSet(Set(0, 1, 2, 10, 20, 30))
    union.getWatermark() shouldBe 3
  }

  it should "union right set with larger watermark correctly" in {
    val lhs = IntPrefixSet(Set(0, 2, 10, 30))
    val rhs = IntPrefixSet(Set(0, 1, 2, 10, 20))
    val union = lhs.union(rhs)
    union shouldBe IntPrefixSet(Set(0, 1, 2, 10, 20, 30))
    union.getWatermark() shouldBe 3
  }

  it should "union with itself correctly" in {
    val gen = Gen.containerOf[Set, Int](Gen.choose(0, 1000000))
    forAll(gen) { (xs: Set[Int]) =>
      val prefixSet = IntPrefixSet(xs)
      val union = prefixSet.union(prefixSet)
      prefixSet shouldBe union
      union.materialize() shouldBe xs
      xs.forall(union.contains) shouldBe true
    }
  }

  it should "union random sets correctly" in {
    forAll(twoGen) { (params: TwoParams) =>
      val TwoParams(lhs, rhs) = params
      val lhsPrefixSet = IntPrefixSet(lhs)
      val rhsPrefixSet = IntPrefixSet(rhs)
      val union = lhsPrefixSet.union(rhsPrefixSet)
      union.materialize() shouldBe lhs.union(rhs)
    }
  }

  it should "diff disjoint sets correctly" in {
    val lhs = IntPrefixSet(Set(10, 20, 30))
    val rhs = IntPrefixSet(Set(0, 1, 2))
    val diff = lhs.diff(rhs)
    diff shouldBe IntPrefixSet(Set(10, 20, 30))
    diff.getWatermark() shouldBe 0
  }

  it should "diff left set with larger watermark correctly" in {
    val lhs = IntPrefixSet(Set(0, 1, 2, 10, 20))
    val rhs = IntPrefixSet(Set(0, 2, 10, 30))
    val diff = lhs.diff(rhs)
    diff shouldBe IntPrefixSet(Set(1, 20))
    diff.getWatermark() shouldBe 0
  }

  it should "diff right set with larger watermark correctly" in {
    val lhs = IntPrefixSet(Set(0, 2, 10, 30))
    val rhs = IntPrefixSet(Set(0, 1, 2, 10, 20))
    val diff = lhs.diff(rhs)
    diff shouldBe IntPrefixSet(Set(30))
    diff.getWatermark() shouldBe 0
  }

  it should "diff with itself correctly" in {
    val gen = Gen.containerOf[Set, Int](Gen.choose(0, 1000000))
    forAll(gen) { (xs: Set[Int]) =>
      val prefixSet = IntPrefixSet(xs)
      val diff = prefixSet.diff(prefixSet)
      diff shouldBe IntPrefixSet()
      diff.materialize() shouldBe Set()
      xs.exists(diff.contains) shouldBe false
    }
  }

  it should "diff random sets correctly" in {
    forAll(twoGen) { (params: TwoParams) =>
      val TwoParams(lhs, rhs) = params
      val lhsPrefixSet = IntPrefixSet(lhs)
      val rhsPrefixSet = IntPrefixSet(rhs)
      val diff = lhsPrefixSet.diff(rhsPrefixSet)
      diff.materialize() shouldBe lhs.diff(rhs)
    }
  }

  it should "addAll random sets correctly" in {
    forAll(twoGen) { (params: TwoParams) =>
      val TwoParams(lhs, rhs) = params
      val lhsPrefixSet = IntPrefixSet(lhs)
      val rhsPrefixSet = IntPrefixSet(rhs)
      lhsPrefixSet.addAll(rhsPrefixSet)
      lhsPrefixSet.materialize() shouldBe lhs.union(rhs)
    }
  }

  it should "subtractAll random sets correctly" in {
    forAll(twoGen) { (params: TwoParams) =>
      val TwoParams(lhs, rhs) = params
      val lhsPrefixSet = IntPrefixSet(lhs)
      val rhsPrefixSet = IntPrefixSet(rhs)
      lhsPrefixSet.subtractAll(rhsPrefixSet)
      lhsPrefixSet.materialize() shouldBe lhs.diff(rhs)
    }
  }

  it should "subtractOne random sets correctly" in {
    val gen = for {
      set <- oneGen
      x <- Gen.chooseNum(0, 100)
    } yield (set.set, x)

    forAll(gen) { (params: (Set[Int], Int)) =>
      val (set, x) = params
      val prefixSet = IntPrefixSet(set)
      prefixSet.subtractOne(x)
      prefixSet.materialize() shouldBe set - x
    }
  }

  it should "proto correctly" in {
    val gen = Gen.containerOf[Set, Int](Gen.choose(0, 1000000))
    forAll(gen) { (xs: Set[Int]) =>
      val prefixSet = IntPrefixSet(xs)
      IntPrefixSet.fromProto(prefixSet.toProto()) shouldBe prefixSet
    }
  }

  "An IntPrefixSet" should "clone correctly" in {
    val prefixSet = IntPrefixSet()
    prefixSet.add(0)
    prefixSet.add(1)
    prefixSet.add(2)
    prefixSet.add(3)

    val clone = prefixSet.clone()
    clone.subtractOne(0)
    clone.subtractOne(1)
    clone.subtractOne(2)
    clone.subtractOne(3)

    prefixSet.contains(0) shouldBe true
    prefixSet.contains(1) shouldBe true
    prefixSet.contains(2) shouldBe true
    prefixSet.contains(3) shouldBe true
    clone.contains(0) shouldBe false
    clone.contains(1) shouldBe false
    clone.contains(2) shouldBe false
    clone.contains(3) shouldBe false
  }
}
