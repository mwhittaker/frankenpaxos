package frankenpaxos.mencius

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class Mencius {
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
      Seq(
        JsTransportAddress("Leader A1"),
        JsTransportAddress("Leader A2"),
        JsTransportAddress("Leader A3")
      ),
      Seq(
        JsTransportAddress("Leader B1"),
        JsTransportAddress("Leader B2"),
        JsTransportAddress("Leader B3")
      )
    ),
    leaderElectionAddresses = Seq(
      Seq(
        JsTransportAddress("LeaderElection A1"),
        JsTransportAddress("LeaderElection A2"),
        JsTransportAddress("LeaderElection A3")
      ),
      Seq(
        JsTransportAddress("LeaderElection B1"),
        JsTransportAddress("LeaderElection B2"),
        JsTransportAddress("LeaderElection B3")
      )
    ),
    proxyLeaderAddresses = Seq(
      JsTransportAddress("ProxyLeader 1"),
      JsTransportAddress("ProxyLeader 2")
    ),
    acceptorAddresses = Seq(
      Seq(
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
      Seq(
        Seq(
          JsTransportAddress("Acceptor C1"),
          JsTransportAddress("Acceptor C2"),
          JsTransportAddress("Acceptor C3")
        )
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
        batchSize = 2
      ),
      metrics = new BatcherMetrics(FakeCollectors)
    )
  }
  val batcher1 = batchers(0)
  val batcher2 = batchers(1)

  // Leaders.
  val leaders = for (group <- config.leaderAddresses; address <- group) yield {
    new Leader[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = LeaderOptions.default.copy(
        sendHighWatermarkEveryN = 2,
        sendNoopRangeIfLaggingBy = 3,
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
  val leaderA1 = leaders(0)
  val leaderA2 = leaders(1)
  val leaderA3 = leaders(2)
  val leaderB1 = leaders(3)
  val leaderB2 = leaders(4)
  val leaderB3 = leaders(5)

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
  val acceptors = for {
    groups <- config.acceptorAddresses
    group <- groups
    address <- group
  } yield {
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
  val acceptorC1 = acceptors(6)
  val acceptorC2 = acceptors(7)
  val acceptorC3 = acceptors(8)

  // Replicas.
  val replicas = for (address <- config.replicaAddresses) yield {
    new Replica[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      stateMachine = new AppendLog(),
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
@JSExportTopLevel("frankenpaxos.mencius.Mencius")
object Mencius {
  val Mencius = new Mencius();
}
