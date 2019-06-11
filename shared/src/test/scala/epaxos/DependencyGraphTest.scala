package frankenpaxos.epaxos

import com.google.protobuf.ByteString
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DependencyGraphTest extends FlatSpec with Matchers {
  private def instance(x: Int): Instance = Instance(x, x)

  private def deps(xs: Int*): Set[Instance] = xs.toSet[Int].map(instance)

  private def instances(xs: Int*): Seq[Instance] = xs.toSeq.map(instance)

  private def runTest(test: (DependencyGraph) => Unit): Unit = {
    test(new JgraphtDependencyGraph())
    test(new ScalaGraphDependencyGraph())
  }

  "A dep graph" should "correctly commit a command with no dependencies" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 0, deps()) shouldBe instances(0)
    }
    runTest(test)
  }

  it should "ignore repeated commands" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 0, deps()) shouldBe instances(0)
      graph.commit(instance(0), 1, deps()) shouldBe instances()
      graph.commit(instance(0), 2, deps(1)) shouldBe instances()
    }
    runTest(test)
  }

  it should "correctly commit a chain of commands" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 0, deps()) shouldBe instances(0)
      graph.commit(instance(1), 0, deps(0)) shouldBe instances(1)
      graph.commit(instance(2), 0, deps(1)) shouldBe instances(2)
      graph.commit(instance(3), 0, deps(2)) shouldBe instances(3)
    }
    runTest(test)
  }

  it should "correctly commit a reverse chain of commands" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(3), 0, deps(2)) shouldBe instances()
      graph.commit(instance(2), 0, deps(1)) shouldBe instances()
      graph.commit(instance(1), 0, deps(0)) shouldBe instances()
      graph.commit(instance(0), 0, deps()) shouldBe instances(0, 1, 2, 3)
    }
    runTest(test)
  }

  it should "correctly commit a reverse chain with sequence numbers" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(3), 0, deps(2)) shouldBe instances()
      graph.commit(instance(2), 1, deps(1)) shouldBe instances()
      graph.commit(instance(1), 2, deps(0)) shouldBe instances()
      graph.commit(instance(0), 3, deps()) shouldBe instances(0, 1, 2, 3)
    }
    runTest(test)
  }

  it should "correctly commit a two cycle" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 0, deps(1)) shouldBe instances()
      graph.commit(instance(1), 0, deps(0)) should
        contain theSameElementsAs instances(0, 1)
    }
    runTest(test)
  }

  it should "correctly commit a two cycle with sequence numbers" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 1, deps(1)) shouldBe instances()
      graph.commit(instance(1), 0, deps(0)) shouldBe instances(1, 0)
    }
    runTest(test)
  }

  it should "correctly commit a three cycle" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 0, deps(1)) shouldBe instances()
      graph.commit(instance(1), 0, deps(2)) shouldBe instances()
      graph.commit(instance(2), 0, deps(0)) should
        contain theSameElementsAs instances(0, 1, 2)
    }
    runTest(test)
  }

  it should "correctly commit a three cycle with sequence numbers" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 1, deps(1)) shouldBe instances()
      graph.commit(instance(1), 0, deps(2)) shouldBe instances()
      graph.commit(instance(2), 2, deps(0)) shouldBe instances(1, 0, 2)
    }
    runTest(test)
  }

  // +---+     +---+     +---+     +---+
  // | 0 | <-- | 1 | <-- | 3 | <-- | 5 |
  // +---+     +---+     +---+     +---+
  //            | ^     /         / | ^
  //            v |   /         /   v |
  //           +---+ l   +---+ l   +---+
  //           | 2 | <-- | 4 | <-- | 6 |
  //           +---+     +---+     +---+
  it should "correctly commit a complex graph in order" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(0), 0, deps()) shouldBe instances(0)
      graph.commit(instance(1), 0, deps(0, 2)) shouldBe instances()
      graph.commit(instance(2), 1, deps(1)) shouldBe instances(1, 2)
      graph.commit(instance(3), 0, deps(1, 2)) shouldBe instances(3)
      graph.commit(instance(4), 0, deps(2)) shouldBe instances(4)
      graph.commit(instance(5), 0, deps(3, 4, 6)) shouldBe instances()
      graph.commit(instance(6), 1, deps(4, 5)) shouldBe instances(5, 6)
    }
    runTest(test)
  }

  it should "correctly commit a complex graph in reverse order" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(6), 1, deps(4, 5)) shouldBe instances()
      graph.commit(instance(5), 0, deps(3, 4, 6)) shouldBe instances()
      graph.commit(instance(4), 0, deps(2)) shouldBe instances()
      graph.commit(instance(3), 0, deps(1, 2)) shouldBe instances()
      graph.commit(instance(2), 1, deps(1)) shouldBe instances()
      graph.commit(instance(1), 0, deps(0, 2)) shouldBe instances()
      Set(
        instances(0, 1, 2, 3, 4, 5, 6),
        instances(0, 1, 2, 4, 3, 5, 6)
      ) should contain(graph.commit(instance(0), 0, deps()))
    }
    runTest(test)
  }

  it should "correctly commit a complex graph in random order" in {
    def test(graph: DependencyGraph): Unit = {
      graph.commit(instance(6), 1, deps(4, 5)) shouldBe instances()
      graph.commit(instance(4), 0, deps(2)) shouldBe instances()
      graph.commit(instance(0), 0, deps()) shouldBe instances(0)
      graph.commit(instance(2), 1, deps(1)) shouldBe instances()
      graph.commit(instance(5), 0, deps(3, 4, 6)) shouldBe instances()
      graph.commit(instance(1), 0, deps(0, 2)) shouldBe instances(1, 2, 4)
      graph.commit(instance(3), 0, deps(1, 2)) shouldBe instances(3, 5, 6)
    }
    runTest(test)
  }
}
