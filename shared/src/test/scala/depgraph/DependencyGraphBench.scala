package frankenpaxos.depgraph

import frankenpaxos.compact.FakeCompactSet
import frankenpaxos.simplebpaxos.VertexId
import frankenpaxos.simplebpaxos.VertexIdHelpers.vertexIdOrdering
import frankenpaxos.util
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object DependencyGraphBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  sealed trait GraphType
  case object Jgrapht extends GraphType
  case object ScalaGraph extends GraphType
  case object Tarjan extends GraphType
  case object IncrementalTarjan extends GraphType

  private def makeGraph(
      t: GraphType
  ): DependencyGraph[VertexId, Unit, FakeCompactSet[VertexId]] = {
    t match {
      case Jgrapht =>
        new JgraphtDependencyGraph[VertexId, Unit, FakeCompactSet[
          VertexId
        ]](new FakeCompactSet[VertexId]())
      case ScalaGraph =>
        new ScalaGraphDependencyGraph[VertexId, Unit, FakeCompactSet[
          VertexId
        ]](new FakeCompactSet[VertexId]())
      case Tarjan =>
        new TarjanDependencyGraph[VertexId, Unit, FakeCompactSet[
          VertexId
        ]](new FakeCompactSet[VertexId]())
      case IncrementalTarjan =>
        new IncrementalTarjanDependencyGraph[
          VertexId,
          Unit,
          FakeCompactSet[
            VertexId
          ]
        ](new FakeCompactSet[VertexId]())
    }
  }

  performance of "JgraphtDependencyGraph commit" in {
    case class Params(
        graphType: GraphType,
        numCommands: Int,
        depSize: Int
    )

    val params =
      for {
        graphType <- Gen.enumeration("graph_type")(Jgrapht,
                                                   Tarjan,
                                                   IncrementalTarjan)
        numCommands <- Gen.enumeration("num_commands")(10000)
        depSize <- Gen.enumeration("dep_size")(1, 10, 25)
      } yield Params(graphType, numCommands, depSize)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val g = makeGraph(params.graphType)
      for (i <- 0 until params.numCommands) {
        val deps = for (d <- i - params.depSize until i if d >= 0)
          yield VertexId(d, d)
        g.commit(VertexId(i, i), (), new FakeCompactSet(deps.toSet))
      }
    }
  }

  performance of "JgraphtDependencyGraph with cycles" in {
    case class Params(
        graphType: GraphType,
        numCommands: Int,
        cycleSize: Int,
        batchSize: Int
    )

    val params =
      for {
        graphType <- Gen.enumeration("graph_type")(Jgrapht,
                                                   Tarjan,
                                                   IncrementalTarjan)
        numCommands <- Gen.enumeration("num_commands")(10000)
        cycleSize <- Gen.enumeration("cycle_size")(1, 10, 25)
        batchSize <- Gen.enumeration("batch_size")(1, 100, 1000)
      } yield Params(graphType, numCommands, cycleSize, batchSize)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val g = makeGraph(params.graphType)
      for {
        i <- 0 until params.numCommands by params.cycleSize
        j <- 0 until params.cycleSize
      } {
        val deps = for (d <- i until i + params.cycleSize if d != i + j)
          yield VertexId(d, d)
        g.commit(VertexId(i + j, i + j), (), new FakeCompactSet(deps.toSet))
        if ((i + 1) % params.batchSize == 0) {
          g.execute()
        }
      }
    }
  }

  performance of "JgraphtDependencyGraph without cycles" in {
    case class Params(
        graphType: GraphType,
        numCommands: Int,
        depSize: Int,
        batchSize: Int
    )

    val params =
      for {
        graphType <- Gen.enumeration("graph_type")(Jgrapht,
                                                   Tarjan,
                                                   IncrementalTarjan)
        numCommands <- Gen.enumeration("num_commands")(10000)
        depSize <- Gen.enumeration("depSize")(1, 10, 25)
        batchSize <- Gen.enumeration("batch_size")(1, 100)
      } yield Params(graphType, numCommands, depSize, batchSize)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val g = makeGraph(params.graphType)
      for (i <- 0 until params.numCommands) {
        val deps = for (d <- i - params.depSize until i if d >= 0)
          yield VertexId(d, d)
        g.commit(VertexId(i, i), (), new FakeCompactSet(deps.toSet))
        if ((i + 1) % params.batchSize == 0) {
          g.execute()
        }
      }
    }
  }
}
