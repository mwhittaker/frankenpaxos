package frankenpaxos.simplegcbpaxos

import frankenpaxos.statemachine.TopK
import frankenpaxos.statemachine.TopOne
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class VertexIdPrefixSetTest extends FlatSpec with Matchers {
  "A VertexIdPrefixSet" should "clone correctly" in {
    val vertices = VertexIdPrefixSet(3)
    vertices.add(new VertexId(0, 0))
    vertices.add(new VertexId(1, 1))
    vertices.add(new VertexId(2, 2))

    val cloned = vertices.clone()
    cloned.subtractOne(new VertexId(0, 0))
    cloned.subtractOne(new VertexId(1, 1))
    cloned.subtractOne(new VertexId(2, 2))

    vertices.contains(new VertexId(0, 0)) shouldBe true
    vertices.contains(new VertexId(1, 1)) shouldBe true
    vertices.contains(new VertexId(2, 2)) shouldBe true
    cloned.contains(new VertexId(0, 0)) shouldBe false
    cloned.contains(new VertexId(1, 1)) shouldBe false
    cloned.contains(new VertexId(2, 2)) shouldBe false
  }

  it should "apply from watermarks correctly" in {
    val vertices = VertexIdPrefixSet(Seq(0, 1, 2, 3))
    vertices.materialize() shouldBe Set(
      new VertexId(1, 0),
      new VertexId(2, 0),
      new VertexId(2, 1),
      new VertexId(3, 0),
      new VertexId(3, 1),
      new VertexId(3, 2)
    )
  }

  it should "fromWatermarksAndSet correctly" in {
    val vertices = VertexIdPrefixSet.fromWatermarksAndSet(
      Seq(0, 1, 2),
      Set(new VertexId(0, 10), new VertexId(1, 1))
    )
    vertices.materialize() shouldBe Set(
      new VertexId(0, 10),
      new VertexId(1, 0),
      new VertexId(1, 1),
      new VertexId(2, 0),
      new VertexId(2, 1)
    )
  }

  it should "fromTopOne correctly" in {
    val topOne = new TopOne(3, VertexIdHelpers.like)
    topOne.put(VertexId(0, 1))
    topOne.put(VertexId(2, 3))
    val vertices = VertexIdPrefixSet.fromTopOne(topOne)
    vertices.materialize() shouldBe
      Set(new VertexId(0, 0), new VertexId(0, 1)) ++
        Set() ++
        Set(new VertexId(2, 0),
            new VertexId(2, 1),
            new VertexId(2, 2),
            new VertexId(2, 3))
  }

  it should "fromTopK correctly" in {
    val topK = new TopK(k = 2, numLeaders = 4, VertexIdHelpers.like)
    topK.put(VertexId(0, 2))
    topK.put(VertexId(0, 10))
    topK.put(VertexId(1, 0))
    topK.put(VertexId(1, 1))
    topK.put(VertexId(1, 5))
    topK.put(VertexId(3, 1))
    topK.put(VertexId(3, 5))
    topK.put(VertexId(3, 10))
    val vertices = VertexIdPrefixSet.fromTopK(topK)
    vertices.materialize() shouldBe
      Set(new VertexId(0, 0),
          new VertexId(0, 1),
          new VertexId(0, 2),
          new VertexId(0, 10)) ++
        Set(new VertexId(1, 0), new VertexId(1, 1), new VertexId(1, 5)) ++
        Set(new VertexId(3, 0),
            new VertexId(3, 1),
            new VertexId(3, 2),
            new VertexId(3, 3),
            new VertexId(3, 4),
            new VertexId(3, 5),
            new VertexId(3, 10))
  }
}
