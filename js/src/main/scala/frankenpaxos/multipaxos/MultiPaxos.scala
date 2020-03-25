package frankenpaxos.multipaxos

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.ReadableAppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class MultiPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    batcherAddresses = Seq(
      JsTransportAddress("Batcher 1"),
      JsTransportAddress("Batcher 2")
    ),
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2")
    ),
    leaderElectionAddresses = Seq(
      JsTransportAddress("LeaderElection 1"),
      JsTransportAddress("LeaderElection 2")
    ),
    proxyLeaderAddresses = Seq(
      JsTransportAddress("ProxyLeader 1"),
      JsTransportAddress("ProxyLeader 2")
    ),
    acceptorAddresses = Seq(
      Seq(
        JsTransportAddress("Acceptor A1"),
        JsTransportAddress("Acceptor A2"),
        JsTransportAddress("Acceptor A3")
      ),
      Seq(
        JsTransportAddress("Acceptor B1"),
        JsTransportAddress("Acceptor B2"),
        JsTransportAddress("Acceptor B3")
      )
    ),
    replicaAddresses = Seq(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2")
    ),
    proxyReplicaAddresses = Seq(
      JsTransportAddress("ProxyReplica 1"),
      JsTransportAddress("ProxyReplica 2")
    ),
    distributionScheme = Hash
  )

  // Clients.
  val clients = for (i <- 1 to 4) yield {
    new Client[JsTransport](
      address = JsTransportAddress(s"Client $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ClientOptions.default.copy(
        resendClientRequestPeriod = java.time.Duration.ofSeconds(30)
      ),
      metrics = new ClientMetrics(FakeCollectors)
    )
  }
  val client1 = clients(0)
  val client2 = clients(1)
  val client3 = clients(2)
  val client4 = clients(3)

  // Batchers.
  val batchers = for (address <- config.batcherAddresses) yield {
    new Batcher[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = BatcherOptions.default.copy(
        batchSize = 1
      ),
      metrics = new BatcherMetrics(FakeCollectors)
    )
  }
  val batcher1 = batchers(0)
  val batcher2 = batchers(1)

  // Leaders.
  val leaders = for (address <- config.leaderAddresses) yield {
    new Leader[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = LeaderOptions.default.copy(
        resendPhase1asPeriod = java.time.Duration.ofSeconds(10),
        electionOptions = ElectionOptions.default.copy(
          pingPeriod = java.time.Duration.ofSeconds(60),
          noPingTimeoutMin = java.time.Duration.ofSeconds(120),
          noPingTimeoutMax = java.time.Duration.ofSeconds(240)
        )
      ),
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }
  val leader1 = leaders(0)
  val leader2 = leaders(1)

  // ProxyLeaders.
  val proxyLeaders = for (address <- config.proxyLeaderAddresses) yield {
    new ProxyLeader[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ProxyLeaderOptions.default,
      metrics = new ProxyLeaderMetrics(FakeCollectors)
    )
  }
  val proxyLeader1 = proxyLeaders(0)
  val proxyLeader2 = proxyLeaders(1)

  // Acceptors.
  val acceptors = for (group <- config.acceptorAddresses; address <- group)
    yield {
      new Acceptor[JsTransport](
        address = address,
        transport = transport,
        logger = new JsLogger(),
        config = config,
        options = AcceptorOptions.default,
        metrics = new AcceptorMetrics(FakeCollectors)
      )
    }
  val acceptorA1 = acceptors(0)
  val acceptorA2 = acceptors(1)
  val acceptorA3 = acceptors(2)
  val acceptorB1 = acceptors(3)
  val acceptorB2 = acceptors(4)
  val acceptorB3 = acceptors(5)

  // Replicas.
  val replicas = for (address <- config.replicaAddresses) yield {
    new Replica[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      stateMachine = new ReadableAppendLog(),
      config = config,
      options = ReplicaOptions.default.copy(
        logGrowSize = 5,
        unsafeDontUseClientTable = false,
        sendChosenWatermarkEveryNEntries = 5,
        recoverLogEntryMinPeriod = java.time.Duration.ofSeconds(60),
        recoverLogEntryMaxPeriod = java.time.Duration.ofSeconds(120),
        unsafeDontRecover = false
      ),
      metrics = new ReplicaMetrics(FakeCollectors)
    )
  }
  val replica1 = replicas(0)
  val replica2 = replicas(1)

  // ProxyReplicas.
  val proxyReplicas = for (address <- config.proxyReplicaAddresses) yield {
    new ProxyReplica[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ProxyReplicaOptions.default,
      metrics = new ProxyReplicaMetrics(FakeCollectors)
    )
  }
  val proxyReplica1 = proxyReplicas(0)
  val proxyReplica2 = proxyReplicas(1)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.multipaxos.MultiPaxos")
object MultiPaxos {
  val MultiPaxos = new MultiPaxos();
}
