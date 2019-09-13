package frankenpaxos.depgraph

import com.google.protobuf.ByteString
import frankenpaxos.compact.IntPrefixSet
import frankenpaxos.monitoring.FakeCollectors
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DependencyGraphTest extends FlatSpec with Matchers with PropertyChecks {
  private val tarjanMetrics = new TarjanDependencyGraphMetrics(FakeCollectors)
  private def jgraph =
    new JgraphtDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet())
  private def scala =
    new ScalaGraphDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet())
  private def tarjan =
    new TarjanDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet(),
                                                      tarjanMetrics)
  private def incremental =
    new IncrementalTarjanDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet())

  // All dependency graphs /////////////////////////////////////////////////////
  "A dep graph" should "correctly commit a command with no dependencies" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0))
      graph.executeByComponent(None)._1 shouldBe Seq()
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "ignore repeated commands" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0))
      graph.commit(0, 1, IntPrefixSet(Set()))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(0, 2, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a chain of commands" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0))
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(1))
      graph.commit(2, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(2))
      graph.commit(3, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(3))
      graph.executeByComponent(None)._1 shouldBe Seq()
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a reverse chain of commands" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(3, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(2, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0),
                                                     Seq(1),
                                                     Seq(2),
                                                     Seq(3))
      graph.executeByComponent(None)._1 shouldBe Seq()
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a reverse chain with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(3, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 2, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(0, 3, IntPrefixSet(Set()))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0),
                                                     Seq(1),
                                                     Seq(2),
                                                     Seq(3))
      graph.executeByComponent(None)._1 shouldBe Seq()
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a two cycle" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0, 1))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a two cycle with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(1, 0))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a three cycle" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 0, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(2, 0, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0, 1, 2))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a three cycle with sequence numbers" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(0, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(2, 2, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(1, 0, 2))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
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
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0))
      graph.commit(1, 0, IntPrefixSet(Set(0, 2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(1, 2))
      graph.commit(3, 0, IntPrefixSet(Set(1, 2)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(3))
      graph.commit(4, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(4))
      graph.commit(5, 0, IntPrefixSet(Set(3, 4, 6)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(6, 1, IntPrefixSet(Set(4, 5)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(5, 6))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a complex graph in reverse order" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(6, 1, IntPrefixSet(Set(4, 5)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(5, 0, IntPrefixSet(Set(3, 4, 6)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(4, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(3, 0, IntPrefixSet(Set(1, 2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0, 2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(0, 0, IntPrefixSet(Set()))
      Set(
        Seq(Seq(0), Seq(1, 2), Seq(3), Seq(4), Seq(5, 6)),
        Seq(Seq(0), Seq(1, 2), Seq(4), Seq(3), Seq(5, 6))
      ) should contain(graph.executeByComponent(None)._1)
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "correctly commit a complex graph in random order" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(6, 1, IntPrefixSet(Set(4, 5)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(4, 0, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(0))
      graph.commit(2, 1, IntPrefixSet(Set(1)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(5, 0, IntPrefixSet(Set(3, 4, 6)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.commit(1, 0, IntPrefixSet(Set(0, 2)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(1, 2), Seq(4))
      graph.commit(3, 0, IntPrefixSet(Set(1, 2)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(3), Seq(5, 6))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
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
      ) should contain(graph.executeByComponent(None)._1)
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    test(incremental)
  }

  it should "handle a simple updateExecuted correctly" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(1, 1, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.updateExecuted(IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(1))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    // TODO(mwhittaker): Test incremental.
    // test(incremental)
  }

  it should "handle a chain of updateExecuted correctly" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(1, 1, IntPrefixSet(Set(0)))
      graph.commit(2, 2, IntPrefixSet(Set(1)))
      graph.commit(3, 3, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.updateExecuted(IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(1), Seq(2), Seq(3))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    // TODO(mwhittaker): Test incremental.
    // test(incremental)
  }

  it should "handle a star of updateExecuted correctly" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(1, 1, IntPrefixSet(Set(0)))
      graph.commit(2, 2, IntPrefixSet(Set(0)))
      graph.commit(3, 3, IntPrefixSet(Set(0)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.updateExecuted(IntPrefixSet(Set(0)))
      Set(
        Seq(Seq(1), Seq(2), Seq(3)),
        Seq(Seq(1), Seq(3), Seq(2)),
        Seq(Seq(2), Seq(1), Seq(3)),
        Seq(Seq(2), Seq(3), Seq(1)),
        Seq(Seq(3), Seq(1), Seq(2)),
        Seq(Seq(3), Seq(2), Seq(1))
      ) should contain(graph.executeByComponent(None)._1)
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    // TODO(mwhittaker): Test incremental.
    // test(incremental)
  }

  it should "updateExecuted on a command in the graph" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(1, 1, IntPrefixSet(Set(0)))
      graph.commit(2, 2, IntPrefixSet(Set(1)))
      graph.commit(3, 3, IntPrefixSet(Set(2)))
      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.updateExecuted(IntPrefixSet(Set(0, 1)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(2), Seq(3))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    // TODO(mwhittaker): Test incremental.
    // test(incremental)
  }

  it should "handle a complex updateExecuted example" in {
    def test(graph: DependencyGraph[Int, Int, IntPrefixSet]): Unit = {
      graph.commit(4, 4, IntPrefixSet(Set(0, 1, 5)))
      graph.commit(5, 5, IntPrefixSet(Set(4, 2)))
      graph.commit(6, 6, IntPrefixSet(Set(7)))
      graph.commit(7, 7, IntPrefixSet(Set(3, 5, 6)))

      graph.executeByComponent(None)._1 shouldBe Seq()
      graph.updateExecuted(IntPrefixSet(Set(0, 1, 2, 3, 4, 5)))
      graph.executeByComponent(None)._1 shouldBe Seq(Seq(6, 7))
    }
    test(jgraph)
    test(scala)
    test(tarjan)
    // TODO(mwhittaker): Test incremental.
    // test(incremental)
  }

  "All dep graph implementations" should "agree" in {
    def test[A](
        numVertices: Int,
        maxVertex: Int,
        f: DependencyGraph[Int, Int, IntPrefixSet] => A,
        equals: (A, A) => Unit
    ): Unit = {
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
          new TarjanDependencyGraph[Int, Int, IntPrefixSet](IntPrefixSet(),
                                                            tarjanMetrics)
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
        val jgraphtOutput = f(jgrapht)
        val scalagraphOutput = f(scalagraph)
        val tarjanOutput = f(tarjan)
        val incrementalOutput = f(incremental)
        equals(jgraphtOutput, scalagraphOutput)
        equals(jgraphtOutput, tarjanOutput)
        if (numVertices == maxVertex) {
          equals(jgraphtOutput, incrementalOutput)
        }
      }
    }

    {
      val f = (g: DependencyGraph[Int, Int, IntPrefixSet]) => {
        g.executeByComponent(None)._1
      }
      val equals = (a: Seq[Seq[Int]], b: Seq[Seq[Int]]) => {
        a should contain theSameElementsAs b
        ()
      }
      test[Seq[Seq[Int]]](numVertices = 5, maxVertex = 5, f, equals)
      test[Seq[Seq[Int]]](numVertices = 10, maxVertex = 20, f, equals)
      test[Seq[Seq[Int]]](numVertices = 10, maxVertex = 10, f, equals)
      test[Seq[Seq[Int]]](numVertices = 100, maxVertex = 200, f, equals)
      test[Seq[Seq[Int]]](numVertices = 100, maxVertex = 100, f, equals)
    }
    info("executeByComponent agrees")

    {
      val f = (g: DependencyGraph[Int, Int, IntPrefixSet]) => {
        g.execute(None)._1
      }
      val equals = (a: Seq[Int], b: Seq[Int]) => {
        a should contain theSameElementsAs b
        ()
      }
      test[Seq[Int]](numVertices = 5, maxVertex = 5, f, equals)
      test[Seq[Int]](numVertices = 10, maxVertex = 20, f, equals)
      test[Seq[Int]](numVertices = 10, maxVertex = 10, f, equals)
      test[Seq[Int]](numVertices = 50, maxVertex = 100, f, equals)
      test[Seq[Int]](numVertices = 50, maxVertex = 50, f, equals)
    }
    info("execute agrees")

    {
      val f = (g: DependencyGraph[Int, Int, IntPrefixSet]) => {
        val executables = mutable.Buffer[Int]()
        val blockers = mutable.Set[Int]()
        g.appendExecute(None, executables, blockers)
        executables
      }
      val equals = (a: mutable.Buffer[Int], b: mutable.Buffer[Int]) => {
        a should contain theSameElementsAs b
        ()
      }
      test[mutable.Buffer[Int]](numVertices = 5, maxVertex = 5, f, equals)
      test[mutable.Buffer[Int]](numVertices = 10, maxVertex = 20, f, equals)
      test[mutable.Buffer[Int]](numVertices = 10, maxVertex = 10, f, equals)
      test[mutable.Buffer[Int]](numVertices = 50, maxVertex = 100, f, equals)
      test[mutable.Buffer[Int]](numVertices = 50, maxVertex = 50, f, equals)
    }
    info("appendExecute agrees")
  }

  // TarjanDependencyGraph /////////////////////////////////////////////////////
  "A TarjanDependencyGraph graph" should "report no blockers" in {
    for (numBlockers <- Seq(None, Some(1), Some(10))) {
      val graph = tarjan
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.commit(1, 1, IntPrefixSet(Set(0)))
      graph.commit(2, 2, IntPrefixSet(Set(1)))
      val (executables, blockers) = graph.executeByComponent(numBlockers)
      executables shouldBe Seq(Seq(0), Seq(1), Seq(2))
      blockers shouldBe Set()
    }
  }

  it should "report all blockers" in {
    val graph = tarjan
    graph.commit(0, 0, IntPrefixSet(Set(10)))
    graph.commit(1, 1, IntPrefixSet(Set(20)))
    graph.commit(2, 2, IntPrefixSet(Set(30)))
    val (executables, blockers) = graph.executeByComponent(None)
    executables shouldBe Seq()
    blockers shouldBe Set(10, 20, 30)
  }

  it should "report blockers chain" in {
    val graph = tarjan
    graph.commit(0, 0, IntPrefixSet(Set(1)))
    graph.commit(1, 1, IntPrefixSet(Set(2)))
    graph.commit(2, 2, IntPrefixSet(Set(3)))
    val (executables, blockers) = graph.executeByComponent(None)
    executables shouldBe Seq()
    blockers shouldBe Set(3)
  }

  it should "report 1 blockers" in {
    val graph = tarjan
    graph.commit(0, 0, IntPrefixSet(Set(10)))
    graph.commit(1, 1, IntPrefixSet(Set(20)))
    graph.commit(2, 2, IntPrefixSet(Set(30)))
    val (executables, blockers) = graph.executeByComponent(Some(1))
    executables shouldBe Seq()
    Set(Set(10), Set(20), Set(30)) should contain(blockers)
  }

  it should "report 2 blockers" in {
    val graph = tarjan
    graph.commit(0, 0, IntPrefixSet(Set(10)))
    graph.commit(1, 1, IntPrefixSet(Set(20)))
    graph.commit(2, 2, IntPrefixSet(Set(30)))
    val (executables, blockers) = graph.executeByComponent(Some(2))
    executables shouldBe Seq()
    Set(Set(10, 20), Set(10, 30), Set(20, 30)) should contain(blockers)
  }

  it should "report 3 blockers" in {
    for (numBlockers <- Seq(Some(3), Some(4), Some(100))) {
      val graph = tarjan
      graph.commit(0, 0, IntPrefixSet(Set(10)))
      graph.commit(1, 1, IntPrefixSet(Set(20)))
      graph.commit(2, 2, IntPrefixSet(Set(30)))
      val (executables, blockers) = graph.executeByComponent(numBlockers)
      executables shouldBe Seq()
      blockers shouldBe Set(10, 20, 30)
    }
  }

  it should "report 1 shared blocker" in {
    val graph = tarjan
    graph.commit(0, 0, IntPrefixSet(Set(10)))
    graph.commit(1, 1, IntPrefixSet(Set(10)))
    graph.commit(2, 2, IntPrefixSet(Set(10)))
    val (executables, blockers) = graph.executeByComponent(None)
    executables shouldBe Seq()
    blockers shouldBe Set(10)
  }

  // IncrementalTarjanDependencyGraph //////////////////////////////////////////
  "An IncrementalTarjanDependencyGraph graph" should "report no blockers" in {
    for (numBlockers <- Seq(None, Some(1), Some(10))) {
      val graph = incremental
      graph.commit(0, 0, IntPrefixSet(Set()))
      graph.commit(1, 1, IntPrefixSet(Set(0)))
      graph.commit(2, 2, IntPrefixSet(Set(1)))
      val (executables, blockers) = graph.executeByComponent(numBlockers)
      executables shouldBe Seq(Seq(0), Seq(1), Seq(2))
      blockers shouldBe Set()
    }
  }

  it should "report blockers chain" in {
    val graph = incremental
    graph.commit(0, 0, IntPrefixSet(Set(1)))
    graph.commit(1, 1, IntPrefixSet(Set(2)))
    graph.commit(2, 2, IntPrefixSet(Set(3)))
    val (executables, blockers) = graph.executeByComponent(None)
    executables shouldBe Seq()
    blockers shouldBe Set(3)
  }

  it should "report 1 blockers" in {
    for (numBlockers <- Seq(None, Some(1), Some(2), Some(3), Some(4))) {
      val graph = incremental
      graph.commit(0, 0, IntPrefixSet(Set(10)))
      graph.commit(1, 1, IntPrefixSet(Set(20)))
      graph.commit(2, 2, IntPrefixSet(Set(30)))
      val (executables, blockers) = graph.executeByComponent(Some(1))
      executables shouldBe Seq()
      Set(Set(10), Set(20), Set(30)) should contain(blockers)
    }
  }
}
