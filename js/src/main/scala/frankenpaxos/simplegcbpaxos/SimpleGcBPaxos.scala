package frankenpaxos.simplegcbpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.depgraph.ScalaGraphDependencyGraph
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class SimpleGcBPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2"),
      JsTransportAddress("Leader 3"),
      JsTransportAddress("Leader 4"),
      JsTransportAddress("Leader 5")
    ),
    proposerAddresses = Seq(
      JsTransportAddress("Proposer 1"),
      JsTransportAddress("Proposer 2"),
      JsTransportAddress("Proposer 3"),
      JsTransportAddress("Proposer 4"),
      JsTransportAddress("Proposer 5")
    ),
    depServiceNodeAddresses = Seq(
      JsTransportAddress("Dep Service Node 1"),
      JsTransportAddress("Dep Service Node 2"),
      JsTransportAddress("Dep Service Node 3")
    ),
    acceptorAddresses = Seq(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
    ),
    replicaAddresses = Seq(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2")
    ),
    garbageCollectorAddresses = Seq(
      JsTransportAddress("Garbage Collector 1"),
      JsTransportAddress("Garbage Collector 2")
    )
  )

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    new Client[JsTransport](
      address = JsTransportAddress(s"Client $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ClientOptions.default.copy(
        reproposePeriod = java.time.Duration.ofSeconds(10)
      ),
      metrics = new ClientMetrics(FakeCollectors)
    )
  }
  val client1 = clients(0)
  val client2 = clients(1)
  val client3 = clients(2)

  // Leaders.
  val leaders = for (i <- 1 to config.leaderAddresses.size) yield {
    new Leader[JsTransport](
      address = JsTransportAddress(s"Leader $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = LeaderOptions.default.copy(
        resendDependencyRequestsTimerPeriod = java.time.Duration.ofSeconds(3)
      ),
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }
  val leader1 = leaders(0)
  val leader2 = leaders(1)
  val leader3 = leaders(2)
  val leader4 = leaders(3)
  val leader5 = leaders(4)

  // Proposers.
  val proposers = for (i <- 1 to config.proposerAddresses.size) yield {
    new Proposer[JsTransport](
      address = JsTransportAddress(s"Proposer $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ProposerOptions.default.copy(
        resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(3),
        resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(3)
      ),
      metrics = new ProposerMetrics(FakeCollectors)
    )
  }
  val proposer1 = proposers(0)
  val proposer2 = proposers(1)
  val proposer3 = proposers(2)
  val proposer4 = proposers(3)
  val proposer5 = proposers(4)

  // DepServiceNodes.
  val depServiceNodes = for (i <- 1 to config.depServiceNodeAddresses.size)
    yield {
      new DepServiceNode[JsTransport](
        address = JsTransportAddress(s"Dep Service Node $i"),
        transport = transport,
        logger = new JsLogger(),
        config = config,
        stateMachine = new AppendLog(),
        options = DepServiceNodeOptions.default.copy(
          garbageCollectEveryNCommands = 2
        ),
        metrics = new DepServiceNodeMetrics(FakeCollectors)
      )
    }
  val depServiceNode1 = depServiceNodes(0)
  val depServiceNode2 = depServiceNodes(1)
  val depServiceNode3 = depServiceNodes(2)

  // Acceptors.
  val acceptors = for (i <- 1 to config.acceptorAddresses.size) yield {
    new Acceptor[JsTransport](
      address = JsTransportAddress(s"Acceptor $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = AcceptorOptions.default.copy(
        statesGrowSize = 2
      ),
      metrics = new AcceptorMetrics(FakeCollectors)
    )
  }
  val acceptor1 = acceptors(0)
  val acceptor2 = acceptors(1)
  val acceptor3 = acceptors(2)

  // Replicas.
  val replicas = for (i <- 1 to config.replicaAddresses.size) yield {
    new Replica[JsTransport](
      address = JsTransportAddress(s"Replica $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      stateMachine = new AppendLog(),
      dependencyGraph = new ScalaGraphDependencyGraph(
        VertexIdPrefixSet(config.leaderAddresses.size)
      ),
      options = ReplicaOptions.default.copy(
        recoverVertexTimerMinPeriod = java.time.Duration.ofSeconds(10),
        recoverVertexTimerMaxPeriod = java.time.Duration.ofSeconds(15),
        executeGraphBatchSize = 1,
        executeGraphTimerPeriod = java.time.Duration.ofSeconds(10),
        garbageCollectEveryNCommands = 2
      ),
      metrics = new ReplicaMetrics(FakeCollectors)
    )
  }
  val replica1 = replicas(0)
  val replica2 = replicas(1)

  // GarbageCollectors.
  val garbageCollectors = for (i <- 1 to config.garbageCollectorAddresses.size)
    yield {
      new GarbageCollector[JsTransport](
        address = JsTransportAddress(s"Garbage Collector $i"),
        transport = transport,
        logger = new JsLogger(),
        config = config,
        options = GarbageCollectorOptions.default.copy(),
        metrics = new GarbageCollectorMetrics(FakeCollectors)
      )
    }
  val garbageCollector1 = garbageCollectors(0)
  val garbageCollector2 = garbageCollectors(1)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.simplegcbpaxos.SimpleGcBPaxos")
object SimpleGcBPaxos {
  val SimpleGcBPaxos = new SimpleGcBPaxos();
}
