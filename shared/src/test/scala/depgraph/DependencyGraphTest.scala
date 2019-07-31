package frankenpaxos.depgraph

import com.google.protobuf.ByteString
import frankenpaxos.compact.IntPrefixSet
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DependencyGraphTest extends FlatSpec with Matchers with PropertyChecks {
  // Run a unit test on JgraphtDependencyGraph, ScalaGraphDependencyGraph, and
  // TarjanDependencyGraph. We don't run on IncrementalTarjanDependencyGraph
  // because it behaves differently than the others.
  private def runTest(
      test: (DependencyGraph[Int, Int, IntPrefixSet]) => Unit
  ): Unit = {
    test(new JgraphtDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet()))
    info("JgraphtDependencyGraph passed")
    test(new ScalaGraphDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet()))
    info("ScalaGraphDependencyGraph passed")
    test(new TarjanDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet()))
    info("TarjanDependencyGraph passed")
  }

  "A dep graph" should "correctly commit a command with no dependencies" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq(Seq(0))
      graph.executeByComponent() shouldBe Seq()
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "ignore repeated commands" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq(Seq(0))
      graph.commit(0, 1, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(0, 2, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a chain of commands" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq(Seq(0))
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent() shouldBe Seq(Seq(1))
      graph.commit(2, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq(Seq(2))
      graph.commit(3, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq(Seq(3))
      graph.executeByComponent() shouldBe Seq()
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a reverse chain of commands" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(3, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(2, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq(Seq(0), Seq(1), Seq(2), Seq(3))
      graph.executeByComponent() shouldBe Seq()
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a reverse chain with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(3, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 2, IntPrefixSet(Set(0)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(0, 3, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq(Seq(0), Seq(1), Seq(2), Seq(3))
      graph.executeByComponent() shouldBe Seq()
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a two cycle" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent() shouldBe Seq(Seq(0, 1))
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a two cycle with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent() shouldBe Seq(Seq(1, 0))
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a three cycle" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(2, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent() shouldBe Seq(Seq(0, 1, 2))
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a three cycle with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(2, 2, IntPrefixSet(Set(0)))
      graph.executeByComponent() shouldBe Seq(Seq(1, 0, 2))
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
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
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq(Seq(0))
      graph.commit(1, 0, IntPrefixSet(Set(0, 2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq(Seq(1, 2))
      graph.commit(3, 0, IntPrefixSet(Set(1, 2)))
      graph.executeByComponent() shouldBe Seq(Seq(3))
      graph.commit(4, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq(Seq(4))
      graph.commit(5, 0, IntPrefixSet(Set(3, 4, 6)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(6, 1, IntPrefixSet(Set(4, 5)))
      graph.executeByComponent() shouldBe Seq(Seq(5, 6))
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a complex graph in reverse order" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(6, 1, IntPrefixSet(Set(4, 5)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(5, 0, IntPrefixSet(Set(3, 4, 6)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(4, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(3, 0, IntPrefixSet(Set(1, 2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0, 2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(0, 0, IntPrefixSet(Set()))
      Set(
        Seq(Seq(0), Seq(1, 2), Seq(3), Seq(4), Seq(5, 6)),
        Seq(Seq(0), Seq(1, 2), Seq(4), Seq(3), Seq(5, 6))
      ) should contain(graph.executeByComponent())
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  it should "correctly commit a complex graph in random order" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(6, 1, IntPrefixSet(Set(4, 5)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(4, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent() shouldBe Seq(Seq(0))
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(5, 0, IntPrefixSet(Set(3, 4, 6)))
      graph.executeByComponent() shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0, 2)))
      graph.executeByComponent() shouldBe Seq(Seq(1, 2), Seq(4))
      graph.commit(3, 0, IntPrefixSet(Set(1, 2)))
      graph.executeByComponent() shouldBe Seq(Seq(3), Seq(5, 6))
    }
    runTest(test)
  }

  it should "correctly commit a hard tarjan test case" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set(3, 1)))
      graph.commit(1, 1, IntPrefixSet(Set(2)))
      graph.commit(2, 2, IntPrefixSet(Set(1)))
      graph.commit(3, 3, IntPrefixSet(Set(4)))
      graph.commit(4, 4, IntPrefixSet(Set(2, 3, 5, 6, 7, 8)))
      graph.commit(5, 5, IntPrefixSet(Set(6)))
      graph.commit(6, 6, IntPrefixSet(Set()))
      graph.commit(7, 7, IntPrefixSet(Set(4)))
      graph.commit(8, 8, IntPrefixSet(Set(7)))
      Set(
        Seq(Seq(1, 2), Seq(6), Seq(5), Seq(3, 4, 7, 8), Seq(0)),
        Seq(Seq(6), Seq(1, 2), Seq(5), Seq(3, 4, 7, 8), Seq(0)),
        Seq(Seq(6), Seq(5), Seq(1, 2), Seq(3, 4, 7, 8), Seq(0))
      ) should contain(graph.executeByComponent())
    }
    runTest(test)
    test(
      new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
        IntPrefixSet()
      )
    )
  }

  "All dep graph implementations" should "agree" in {
    def test(numVertices: Int, maxVertex: Int): Unit = {
      require(maxVertex >= numVertices)
      info(s"numVertices = $numVertices, maxVertex = $maxVertex")

      val keysGen = Gen.pick(numVertices, 0 until maxVertex)
      val sequencesGen = Gen.listOfN(numVertices, Gen.chooseNum(0, 1000))
      val depsGen = Gen.listOfN(
        numVertices,
        Gen.choose(0, numVertices).flatMap(Gen.pick(_, 0 until maxVertex))
      )
      val nodesGen: Gen[Seq[(Int, Int, Set[Int])]] = for {
        keys <- keysGen
        sequences <- sequencesGen
        deps <- depsGen
        ((k, s), d) <- keys zip sequences zip deps
      } yield (k, s, Set() ++ d)

      forAll(nodesGen) { (nodes: Seq[(Int, Int, Set[Int])]) =>
        val jgrapht =
          new JgraphtDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet())
        val scalagraph =
          new ScalaGraphDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet())
        val tarjan =
          new TarjanDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet())
        val incremental =
          new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](
            IntPrefixSet()
          )
        for ((id, sequenceNumber, dependencies) <- nodes) {
          jgrapht.commit(id, sequenceNumber, IntPrefixSet(dependencies - id))
          scalagraph.commit(id, sequenceNumber, IntPrefixSet(dependencies - id))
          tarjan.commit(id, sequenceNumber, IntPrefixSet(dependencies - id))
          incremental.commit(id,
                             sequenceNumber,
                             IntPrefixSet(dependencies - id))
        }

        // If numVertices is equal to maxVertex, then every dependency is
        // committed. In this case, all four graph algorithms should behave
        // identically. Otherwise, all the graphs except for
        // IncrementalTarjanDependencyGraph behave identically.
        //
        // Note that we check that the graphs have the same elements. It's
        // possible that they return elements in different orders, though, so
        // they may not be exactly equal.
        val jgraphtOutput = jgrapht.executeByComponent()
        val scalagraphOutput = scalagraph.executeByComponent()
        val tarjanOutput = tarjan.executeByComponent()
        val incrementalOutput = incremental.executeByComponent()
        jgraphtOutput should contain theSameElementsAs scalagraphOutput
        jgraphtOutput should contain theSameElementsAs tarjanOutput
        if (numVertices == maxVertex) {
          jgraphtOutput should contain theSameElementsAs incrementalOutput
        }
      }
    }

    test(numVertices = 10, maxVertex = 20)
    test(numVertices = 10, maxVertex = 10)
    test(numVertices = 100, maxVertex = 200)
    test(numVertices = 100, maxVertex = 100)
  }
}
