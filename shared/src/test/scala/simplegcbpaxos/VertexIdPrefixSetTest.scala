package frankenpaxos.simplegcbpaxos

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

  it should "fromTopK correctly" in {
    val vertices = VertexIdPrefixSet.fromTopK(
      4,
      Set(new VertexId(0, 2), new VertexId(0, 10)) ++
        Set(new VertexId(1, 0), new VertexId(1, 1), new VertexId(1, 5)) ++
        Set(new VertexId(3, 10), new VertexId(3, 5), new VertexId(3, 1))
    )
    vertices.materialize() shouldBe
      Set(new VertexId(0, 0),
          new VertexId(0, 1),
          new VertexId(0, 2),
          new VertexId(0, 10)) ++
        Set(new VertexId(1, 0), new VertexId(1, 1), new VertexId(1, 5)) ++
        Set(new VertexId(3, 0),
            new VertexId(3, 1),
            new VertexId(3, 5),
            new VertexId(3, 10))
  }
}
