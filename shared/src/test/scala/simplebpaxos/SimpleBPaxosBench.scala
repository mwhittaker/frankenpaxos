package frankenpaxos.simplebpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.KeyValueStore
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

object SimpleBPaxosBenchmark extends Bench.ForkedTime {
  override def aggregator: Aggregator[Double] = Aggregator.average

  val params =
    for {
      n <- Gen.enumeration("num_commands")(10000)
      cycleSize <- Gen.enumeration("cycle_size")(1, 5, 10)
    } yield (n, cycleSize)

  performance of "Replica" in {
    using(params) config (
      exec.independentSamples -> 3,
      exec.benchRuns -> 5,
    ) in {
      case (n, cycleSize) =>
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
          dependencyGraph = new JgraphtDependencyGraph(),
          options = ReplicaOptions.default,
          metrics = new ReplicaMetrics(FakeCollectors)
        )

        for (i <- 0 until n by cycleSize) {
          for (j <- 0 until cycleSize) {
            val deps = for (d <- i until i + cycleSize if d != i + j)
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
