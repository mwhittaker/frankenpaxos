package frankenpaxos.depgraph

import com.google.protobuf.ByteString
import frankenpaxos.compact.FakeCompactSet
import frankenpaxos.compact.IntPrefixSet
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.util.VertexIdLike
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ZigzagTarjanDependencyGraphTest extends FlatSpec with Matchers {
  private val like = new VertexIdLike[(Int, Int)] {
    override def leaderIndex(t: (Int, Int)): Int = t._1
    override def id(t: (Int, Int)): Int = t._2
    override def make(x: Int, y: Int): (Int, Int) = (x, y)
  }

  type IntIntZigZag =
    ZigzagTarjanDependencyGraph[(Int, Int), Int, FakeCompactSet[(Int, Int)]]

  private def zigzag(): IntIntZigZag = {
    new ZigzagTarjanDependencyGraph(
      new FakeCompactSet[(Int, Int)](),
      numLeaders = 3,
      like = like,
      options = ZigzagTarjanDependencyGraphOptions(
        verticesGrowSize = 10,
        garbageCollectEveryNCommands = 100
      ),
      metrics = new ZigzagTarjanDependencyGraphMetrics(FakeCollectors)
    )
  }

  private def zigzag(
      options: ZigzagTarjanDependencyGraphOptions
  ): IntIntZigZag = {
    new ZigzagTarjanDependencyGraph(
      new FakeCompactSet[(Int, Int)](),
      numLeaders = 3,
      like = like,
      options = options,
      metrics = new ZigzagTarjanDependencyGraphMetrics(FakeCollectors)
    )
  }

  "A ZigzagTarjanDependencyGraph" should "execute no commands correctly" in {
    val graph = zigzag()
    graph.executeByComponent(None) shouldBe (Seq(), Set((0, 0), (1, 0), (2, 0)))
  }

  it should "execute one command correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set()))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((0, 0))), Set((0, 1), (1, 0), (2, 0)))
  }

  it should "execute a chain of commands correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set()))
    graph.commit((1, 0), 0, new FakeCompactSet(Set((0, 0))))
    graph.commit((2, 0), 0, new FakeCompactSet(Set((1, 0))))
    graph.commit((0, 1), 0, new FakeCompactSet(Set((2, 0))))
    graph.commit((1, 1), 0, new FakeCompactSet(Set((0, 1))))
    graph.commit((2, 1), 0, new FakeCompactSet(Set((1, 1))))
    graph.executeByComponent(None) shouldBe (
      Seq(Seq((0, 0)),
          Seq((1, 0)),
          Seq((2, 0)),
          Seq((0, 1)),
          Seq((1, 1)),
          Seq((2, 1))),
      Set((0, 2), (1, 2), (2, 2))
    )
  }

  it should "execute a region of commands with back edges correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set()))
    graph.commit((1, 0), 0, new FakeCompactSet(Set((0, 0))))
    graph.commit((2, 0), 0, new FakeCompactSet(Set((1, 0), (0, 0))))
    graph.commit((0, 1), 0, new FakeCompactSet(Set((2, 0))))
    graph.commit((1, 1), 0, new FakeCompactSet(Set()))
    graph.commit((2, 1), 0, new FakeCompactSet(Set((1, 1), (2, 0))))
    graph.executeByComponent(None) shouldBe (
      Seq(Seq((0, 0)),
          Seq((1, 0)),
          Seq((2, 0)),
          Seq((0, 1)),
          Seq((1, 1)),
          Seq((2, 1))),
      Set((0, 2), (1, 2), (2, 2))
    )
  }

  it should "execute a forward edge correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set((1, 0))))
    graph.commit((1, 0), 0, new FakeCompactSet(Set()))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((1, 0)), Seq((0, 0))), Set((0, 1), (1, 1), (2, 0)))
  }

  it should "execute a cycle correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set((1, 0))))
    graph.commit((1, 0), 1, new FakeCompactSet(Set((0, 0))))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((0, 0), (1, 0))), Set((0, 1), (1, 1), (2, 0)))
  }

  it should "execute a forward edge with gap correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set((2, 0))))
    graph.commit((2, 0), 1, new FakeCompactSet(Set()))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((2, 0)), Seq((0, 0))), Set((0, 1), (1, 0), (2, 1)))

    graph.commit((1, 0), 0, new FakeCompactSet(Set()))
    graph.commit((0, 1), 1, new FakeCompactSet(Set()))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((0, 1)), Seq((1, 0))), Set((0, 2), (1, 1), (2, 1)))
  }

  it should "garbage collect correctly" in {
    val graph = zigzag(
      ZigzagTarjanDependencyGraphOptions(
        verticesGrowSize = 10,
        garbageCollectEveryNCommands = 3
      )
    )
    graph.commit((0, 0), 0, new FakeCompactSet(Set()))
    graph.commit((1, 0), 0, new FakeCompactSet(Set()))
    graph.commit((2, 0), 0, new FakeCompactSet(Set()))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((0, 0)), Seq((1, 0)), Seq((2, 0))), Set((0, 1), (1, 1), (2, 1)))

    graph.commit((0, 1), 0, new FakeCompactSet(Set((0, 0), (1, 0), (2, 0))))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((0, 1))), Set((0, 2), (1, 1), (2, 1)))
  }

  it should "handle unmet dep correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set((0, 1))))
    graph.executeByComponent(None) shouldBe (Seq(), Set((0, 1), (1, 0), (2, 0)))
    graph.commit((0, 1), 0, new FakeCompactSet(Set((0, 2))))
    graph.executeByComponent(None) shouldBe (Seq(), Set((0, 2), (1, 0), (2, 0)))
    graph.commit((0, 2), 0, new FakeCompactSet(Set((0, 3))))
    graph.executeByComponent(None) shouldBe (Seq(), Set((0, 3), (1, 0), (2, 0)))
    graph.commit((0, 3), 0, new FakeCompactSet(Set()))
    graph.executeByComponent(None) shouldBe
      (Seq(Seq((0, 3)), Seq((0, 2)), Seq((0, 1)), Seq((0, 0))),
      Set((0, 4), (1, 0), (2, 0)))
  }

  it should "handle tall column correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set()))
    graph.commit((1, 0), 0, new FakeCompactSet(Set()))
    graph.commit((1, 1), 0, new FakeCompactSet(Set()))
    graph.commit((1, 2), 0, new FakeCompactSet(Set()))
    graph.commit((1, 3), 0, new FakeCompactSet(Set()))
    graph.executeByComponent(None) shouldBe (
      Seq(Seq((0, 0)), Seq((1, 0)), Seq((1, 1)), Seq((1, 2)), Seq((1, 3))),
      Set((0, 1), (1, 4), (2, 0))
    )
  }

  it should "updateExecuted correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set((0, 10))))
    graph.commit((1, 0), 0, new FakeCompactSet(Set((0, 10))))
    graph.commit((2, 0), 0, new FakeCompactSet(Set((0, 10))))
    graph.executeByComponent(None) shouldBe (Seq(), Set((0, 10)))

    graph.updateExecuted(new FakeCompactSet(Set((0, 0), (1, 0), (2, 0))))
    graph.executeByComponent(None) shouldBe (Seq(), Set((0, 1), (1, 1), (2, 1)))
  }

  it should "compute eligibility correctly" in {
    val graph = zigzag()
    graph.commit((0, 0), 0, new FakeCompactSet(Set((2, 0), (1, 0))))
    graph.commit((2, 0), 0, new FakeCompactSet(Set((0, 0))))
    graph.executeByComponent(None) shouldBe (Seq(), Set((1, 0)))
  }
}
