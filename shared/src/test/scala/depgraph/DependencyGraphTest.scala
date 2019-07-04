package frankenpaxos.depgraph

import com.google.protobuf.ByteString
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DependencyGraphTest extends FlatSpec with Matchers {
  private def runTest(test: (DependencyGraph[Int, Int]) => Unit): Unit = {
    test(new JgraphtDependencyGraph[Int, Int]())
    info("JgraphtDependencyGraph passed")
    test(new ScalaGraphDependencyGraph[Int, Int]())
    info("ScalaGraphDependencyGraph passed")
    test(new TarjanDependencyGraph[Int, Int]())
    info("TarjanDependencyGraph passed")
  }

  "A dep graph" should "correctly commit a command with no dependencies" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 0, Set())
      graph.execute() shouldBe Seq(0)
      graph.execute() shouldBe Seq()
    }
    runTest(test)
  }

  it should "ignore repeated commands" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 0, Set())
      graph.execute() shouldBe Seq(0)
      graph.commit(0, 1, Set())
      graph.execute() shouldBe Seq()
      graph.commit(0, 2, Set(1))
      graph.execute() shouldBe Seq()
    }
    runTest(test)
  }

  it should "correctly commit a chain of commands" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 0, Set())
      graph.execute() shouldBe Seq(0)
      graph.commit(1, 0, Set(0))
      graph.execute() shouldBe Seq(1)
      graph.commit(2, 0, Set(1))
      graph.execute() shouldBe Seq(2)
      graph.commit(3, 0, Set(2))
      graph.execute() shouldBe Seq(3)
      graph.execute() shouldBe Seq()
    }
    runTest(test)
  }

  it should "correctly commit a reverse chain of commands" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(3, 0, Set(2))
      graph.execute() shouldBe Seq()
      graph.commit(2, 0, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(1, 0, Set(0))
      graph.execute() shouldBe Seq()
      graph.commit(0, 0, Set())
      graph.execute() shouldBe Seq(0, 1, 2, 3)
      graph.execute() shouldBe Seq()
    }
    runTest(test)
  }

  it should "correctly commit a reverse chain with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(3, 0, Set(2))
      graph.execute() shouldBe Seq()
      graph.commit(2, 1, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(1, 2, Set(0))
      graph.execute() shouldBe Seq()
      graph.commit(0, 3, Set())
      graph.execute() shouldBe Seq(0, 1, 2, 3)
      graph.execute() shouldBe Seq()
    }
    runTest(test)
  }

  it should "correctly commit a two cycle" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 0, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(1, 0, Set(0))
      graph.execute() should contain theSameElementsAs Set(0, 1)
    }
    runTest(test)
  }

  it should "correctly commit a two cycle with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 1, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(1, 0, Set(0))
      graph.execute() shouldBe Seq(1, 0)
    }
    runTest(test)
  }

  it should "correctly commit a three cycle" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 0, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(1, 0, Set(2))
      graph.execute() shouldBe Seq()
      graph.commit(2, 0, Set(0))
      graph.execute() should contain theSameElementsAs Set(0, 1, 2)
    }
    runTest(test)
  }

  it should "correctly commit a three cycle with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 1, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(1, 0, Set(2))
      graph.execute() shouldBe Seq()
      graph.commit(2, 2, Set(0))
      graph.execute() shouldBe Seq(1, 0, 2)
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
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(0, 0, Set())
      graph.execute() shouldBe Seq(0)
      graph.commit(1, 0, Set(0, 2))
      graph.execute() shouldBe Seq()
      graph.commit(2, 1, Set(1))
      graph.execute() shouldBe Seq(1, 2)
      graph.commit(3, 0, Set(1, 2))
      graph.execute() shouldBe Seq(3)
      graph.commit(4, 0, Set(2))
      graph.execute() shouldBe Seq(4)
      graph.commit(5, 0, Set(3, 4, 6))
      graph.execute() shouldBe Seq()
      graph.commit(6, 1, Set(4, 5))
      graph.execute() shouldBe Seq(5, 6)
    }
    runTest(test)
  }

  it should "correctly commit a complex graph in reverse order" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(6, 1, Set(4, 5))
      graph.execute() shouldBe Seq()
      graph.commit(5, 0, Set(3, 4, 6))
      graph.execute() shouldBe Seq()
      graph.commit(4, 0, Set(2))
      graph.execute() shouldBe Seq()
      graph.commit(3, 0, Set(1, 2))
      graph.execute() shouldBe Seq()
      graph.commit(2, 1, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(1, 0, Set(0, 2))
      graph.execute() shouldBe Seq()
      graph.commit(0, 0, Set())
      Set(
        Seq(0, 1, 2, 3, 4, 5, 6),
        Seq(0, 1, 2, 4, 3, 5, 6)
      ) should contain(graph.execute())
    }
    runTest(test)
  }

  it should "correctly commit a complex graph in random order" in {
    def test(graph: DependencyGraph[Int, Int]): Unit = {
      graph.commit(6, 1, Set(4, 5))
      graph.execute() shouldBe Seq()
      graph.commit(4, 0, Set(2))
      graph.execute() shouldBe Seq()
      graph.commit(0, 0, Set())
      graph.execute() shouldBe Seq(0)
      graph.commit(2, 1, Set(1))
      graph.execute() shouldBe Seq()
      graph.commit(5, 0, Set(3, 4, 6))
      graph.execute() shouldBe Seq()
      graph.commit(1, 0, Set(0, 2))
      graph.execute() shouldBe Seq(1, 2, 4)
      graph.commit(3, 0, Set(1, 2))
      graph.execute() shouldBe Seq(3, 5, 6)
    }
    runTest(test)
  }
}
