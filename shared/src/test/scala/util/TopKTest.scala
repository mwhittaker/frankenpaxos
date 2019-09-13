package frankenpaxos.util

import collection.mutable
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class TopKTest extends FlatSpec with Matchers {
  val intTuple = new VertexIdLike[(Int, Int)] {
    def leaderIndex(t: (Int, Int)): Int = {
      val (x, _) = t
      x
    }

    def id(t: (Int, Int)): Int = {
      val (_, y) = t
      y
    }
  }

  "A TopK" should "return empty set after no puts" in {
    for (k <- 1 until 10) {
      val topK = new TopK(k, numLeaders = 3, intTuple)
      topK.get() shouldBe mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int]()
      )
    }
  }

  it should "return one thing after putting it" in {
    for (k <- 1 until 10) {
      val topK = new TopK(k, numLeaders = 3, intTuple)
      topK.put((0, 0))
      topK.get() shouldBe mutable.Buffer(
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int]()
      )
    }
  }

  it should "return one thing per leader after putting them" in {
    for (k <- 1 until 10) {
      val topK = new TopK(k, numLeaders = 3, intTuple)
      topK.put((0, 0))
      topK.put((1, 1))
      topK.put((2, 2))
      topK.get() shouldBe mutable.Buffer(
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](1),
        mutable.SortedSet[Int](2)
      )
    }
  }

  it should "return top thing per leader after lots of puts" in {
    def put(topK: TopK[(Int, Int)]): TopK[(Int, Int)] = {
      topK.put((0, 0))
      topK.put((0, 1))
      topK.put((0, 2))
      topK.put((1, 1))
      topK.put((1, 10))
      topK.put((2, 2))
      topK.put((4, 3))
      topK.put((4, 1))
      topK.put((4, 7))
      topK.put((4, 1))
      topK
    }

    val top1 = put(new TopK(1, numLeaders = 5, intTuple))
    val top2 = put(new TopK(2, numLeaders = 5, intTuple))
    val top3 = put(new TopK(3, numLeaders = 5, intTuple))
    val top4 = put(new TopK(4, numLeaders = 5, intTuple))

    top1.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](2),
      mutable.SortedSet[Int](10),
      mutable.SortedSet[Int](2),
      mutable.SortedSet[Int](),
      mutable.SortedSet[Int](7)
    )
    top2.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](1, 2),
      mutable.SortedSet[Int](1, 10),
      mutable.SortedSet[Int](2),
      mutable.SortedSet[Int](),
      mutable.SortedSet[Int](3, 7)
    )
    top3.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](0, 1, 2),
      mutable.SortedSet[Int](1, 10),
      mutable.SortedSet[Int](2),
      mutable.SortedSet[Int](),
      mutable.SortedSet[Int](1, 3, 7)
    )
    top4.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](0, 1, 2),
      mutable.SortedSet[Int](1, 10),
      mutable.SortedSet[Int](2),
      mutable.SortedSet[Int](),
      mutable.SortedSet[Int](1, 3, 7)
    )
  }

  it should "merge two empty indexes correctly" in {
    for (k <- 1 until 5) {
      val lhs = new TopK(k, numLeaders = 3, intTuple)
      val rhs = new TopK(k, numLeaders = 3, intTuple)
      lhs.mergeEquals(rhs)
      lhs.get() shouldBe mutable.Buffer(
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int](),
        mutable.SortedSet[Int]()
      )
    }
  }

  it should "merge lhs empty correctly" in {
    for (k <- 1 until 5) {
      val lhs = new TopK(k, numLeaders = 3, intTuple)
      val rhs = new TopK(k, numLeaders = 3, intTuple)
      rhs.put((0, 0))
      rhs.put((1, 1))
      rhs.put((2, 2))
      lhs.mergeEquals(rhs)
      lhs.get() shouldBe mutable.Buffer(
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](1),
        mutable.SortedSet[Int](2)
      )
    }
  }

  it should "merge rhs empty correctly" in {
    for (k <- 1 until 5) {
      val lhs = new TopK(k, numLeaders = 3, intTuple)
      val rhs = new TopK(k, numLeaders = 3, intTuple)
      lhs.put((0, 0))
      lhs.put((1, 1))
      lhs.put((2, 2))
      lhs.mergeEquals(rhs)
      lhs.get() shouldBe mutable.Buffer(
        mutable.SortedSet[Int](0),
        mutable.SortedSet[Int](1),
        mutable.SortedSet[Int](2)
      )
    }
  }

  it should "merge complex example correctly" in {
    def mergeEquals(
        lhs: TopK[(Int, Int)],
        rhs: TopK[(Int, Int)]
    ): TopK[(Int, Int)] = {
      lhs.put((0, 1))
      lhs.put((0, 2))
      lhs.put((0, 3))
      rhs.put((0, 100))
      rhs.put((0, 200))
      rhs.put((0, 300))

      lhs.put((1, 100))
      lhs.put((1, 200))
      lhs.put((1, 300))
      rhs.put((1, 1))
      rhs.put((1, 2))
      rhs.put((1, 3))

      lhs.put((2, 1))
      lhs.put((2, 200))
      lhs.put((2, 3))
      rhs.put((2, 100))
      rhs.put((2, 2))
      rhs.put((2, 300))

      lhs.put((3, 0))
      rhs.put((3, 1))

      lhs.mergeEquals(rhs)
      lhs
    }

    val lhs1 = mergeEquals(new TopK(k = 1, numLeaders = 4, intTuple),
                           new TopK(k = 1, numLeaders = 4, intTuple))
    lhs1.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](300),
      mutable.SortedSet[Int](300),
      mutable.SortedSet[Int](300),
      mutable.SortedSet[Int](1)
    )

    val lhs2 = mergeEquals(new TopK(k = 2, numLeaders = 4, intTuple),
                           new TopK(k = 2, numLeaders = 4, intTuple))
    lhs2.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](200, 300),
      mutable.SortedSet[Int](200, 300),
      mutable.SortedSet[Int](200, 300),
      mutable.SortedSet[Int](0, 1)
    )

    val lhs3 = mergeEquals(new TopK(k = 3, numLeaders = 4, intTuple),
                           new TopK(k = 3, numLeaders = 4, intTuple))
    lhs3.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](100, 200, 300),
      mutable.SortedSet[Int](100, 200, 300),
      mutable.SortedSet[Int](100, 200, 300),
      mutable.SortedSet[Int](0, 1)
    )

    val lhs4 = mergeEquals(new TopK(k = 4, numLeaders = 4, intTuple),
                           new TopK(k = 4, numLeaders = 4, intTuple))
    lhs4.get() shouldBe mutable.Buffer(
      mutable.SortedSet[Int](3, 100, 200, 300),
      mutable.SortedSet[Int](3, 100, 200, 300),
      mutable.SortedSet[Int](3, 100, 200, 300),
      mutable.SortedSet[Int](0, 1)
    )
  }
}
