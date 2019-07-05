package frankenpaxos.simplebpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.depgraph.ScalaGraphDependencyGraph
import frankenpaxos.depgraph.TarjanDependencyGraph
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.KeyValueStore
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scalameter.picklers.noPickler._

object SimpleBPaxosBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  sealed trait GraphType
  case object Jgrapht extends GraphType
  case object ScalaGraph extends GraphType
  case object Tarjan extends GraphType

  private def makeGraph(t: GraphType): DependencyGraph[VertexId, Unit] = {
    t match {
      case Jgrapht    => new JgraphtDependencyGraph[VertexId, Unit]()
      case ScalaGraph => new ScalaGraphDependencyGraph[VertexId, Unit]()
      case Tarjan     => new TarjanDependencyGraph[VertexId, Unit]()
    }
  }

  performance of "Replica with cycles" in {
    case class Params(
        graphType: GraphType,
        numCommands: Int,
        cycleSize: Int
    )

    val params =
      for {
        graphType <- Gen.enumeration("graph_type")(Jgrapht, Tarjan)
        numCommands <- Gen.enumeration("num_commands")(10000)
        cycleSize <- Gen.enumeration("cycle_size")(1, 10, 25)
      } yield Params(graphType, numCommands, cycleSize)

    using(params) config (
      exec.independentSamples -> 1,
      exec.benchRuns -> 1,
    ) in { params =>
      val logger = new FakeLogger()
      val transport = new FakeTransport(logger)
      val config = Config[FakeTransport](
        f = 1,
        leaderAddresses = for (i <- 1 to 2)
          yield FakeTransportAddress(s"Leader $i"),
        proposerAddresses = for (i <- 1 to 2)
          yield FakeTransportAddress(s"Proposer $i"),
        depServiceNodeAddresses = for (i <- 1 to 3)
          yield FakeTransportAddress(s"Dep Service Node $i"),
        acceptorAddresses = for (i <- 1 to 3)
          yield FakeTransportAddress(s"Acceptor $i"),
        replicaAddresses = for (i <- 1 to 2)
          yield FakeTransportAddress(s"Replica $i")
      )
      val replica = new Replica[FakeTransport](
        address = FakeTransportAddress(s"Replica 1"),
        transport = transport,
        logger = logger,
        config = config,
        stateMachine = new KeyValueStore(),
        dependencyGraph = makeGraph(params.graphType),
        options = ReplicaOptions.default,
        metrics = new ReplicaMetrics(FakeCollectors)
      )

      for (i <- 0 until params.numCommands by params.cycleSize) {
        for (j <- 0 until params.cycleSize) {
          val deps = for (d <- i until i + params.cycleSize if d != i + j)
            yield VertexId(d, d)
          replica.handleCommit(
            new FakeTransportAddress("Client 1"),
            Commit(
              vertexId = VertexId(i + j, i + j),
              commandOrNoop = CommandOrNoop().withNoop(Noop()),
              dependency = deps
            )
          )
        }
      }
    }
  }
}
