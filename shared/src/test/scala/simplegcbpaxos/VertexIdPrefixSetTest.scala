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
}
