package frankenpaxos.statemachine

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
      val topK = new TopK(k, intTuple)
      topK.get() shouldBe Set()
    }
  }

  it should "return one thing after putting it" in {
    for (k <- 1 until 10) {
      val topK = new TopK(k, intTuple)
      topK.put((0, 0))
      topK.get() shouldBe Set((0, 0))
    }
  }

  it should "return one thing per leader after putting them" in {
    for (k <- 1 until 10) {
      val topK = new TopK(k, intTuple)
      topK.put((0, 0))
      topK.put((1, 1))
      topK.put((2, 2))
      topK.put((3, 3))
      topK.get() shouldBe Set((0, 0), (1, 1), (2, 2), (3, 3))
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

    val top1 = put(new TopK(1, intTuple))
    val top2 = put(new TopK(2, intTuple))
    val top3 = put(new TopK(3, intTuple))
    val top4 = put(new TopK(4, intTuple))

    top1.get() shouldBe Set((0, 2), (1, 10), (2, 2), (4, 7))
    top2.get() shouldBe
      Set((0, 2), (0, 1), (1, 1), (1, 10), (2, 2), (4, 7), (4, 3))
    top3.get() shouldBe
      Set((0, 0),
          (0, 1),
          (0, 2),
          (1, 1),
          (1, 10),
          (2, 2),
          (4, 1),
          (4, 3),
          (4, 7))
    top4.get() shouldBe
      Set((0, 0),
          (0, 1),
          (0, 2),
          (1, 1),
          (1, 10),
          (2, 2),
          (4, 1),
          (4, 3),
          (4, 7))
  }

  it should "merge two empty indexes correctly" in {
    for (k <- 1 until 5) {
      val lhs = new TopK(k, intTuple)
      val rhs = new TopK(k, intTuple)
      lhs.mergeEquals(rhs)
      lhs.get() shouldBe Set()
    }
  }

  it should "merge lhs empty correctly" in {
    for (k <- 1 until 5) {
      val lhs = new TopK(k, intTuple)
      val rhs = new TopK(k, intTuple)
      rhs.put((0, 0))
      rhs.put((1, 1))
      rhs.put((2, 2))
      lhs.mergeEquals(rhs)
      lhs.get() shouldBe Set((0, 0), (1, 1), (2, 2))
    }
  }

  it should "merge rhs empty correctly" in {
    for (k <- 1 until 5) {
      val lhs = new TopK(k, intTuple)
      val rhs = new TopK(k, intTuple)
      lhs.put((0, 0))
      lhs.put((1, 1))
      lhs.put((2, 2))
      lhs.mergeEquals(rhs)
      lhs.get() shouldBe Set((0, 0), (1, 1), (2, 2))
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

    val lhs1 = mergeEquals(new TopK(k = 1, intTuple), new TopK(k = 1, intTuple))
    lhs1.get() shouldBe Set((0, 300), (1, 300), (2, 300), (3, 1))

    val lhs2 = mergeEquals(new TopK(k = 2, intTuple), new TopK(k = 2, intTuple))
    lhs2.get() shouldBe
      Set((0, 200), (0, 300)) ++ Set((1, 200), (1, 300)) ++
        Set((2, 200), (2, 300)) ++ Set((3, 0), (3, 1))

    val lhs3 = mergeEquals(new TopK(k = 3, intTuple), new TopK(k = 3, intTuple))
    lhs3.get() shouldBe
      Set((0, 100), (0, 200), (0, 300)) ++ Set((1, 100), (1, 200), (1, 300)) ++
        Set((2, 100), (2, 200), (2, 300)) ++ Set((3, 0), (3, 1))

    val lhs4 = mergeEquals(new TopK(k = 4, intTuple), new TopK(k = 4, intTuple))
    lhs4.get() shouldBe
      Set((0, 3), (0, 100), (0, 200), (0, 300)) ++
        Set((1, 3), (1, 100), (1, 200), (1, 300)) ++
        Set((2, 3), (2, 100), (2, 200), (2, 300)) ++
        Set((3, 0), (3, 1))
  }
}
