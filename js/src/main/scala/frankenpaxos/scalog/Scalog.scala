package frankenpaxos.scalog

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.ReadableAppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class Scalog(batch: Boolean, flexible: Boolean) {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    serverAddresses = Seq(
      Seq(JsTransportAddress("Server 1"), JsTransportAddress("Server 2")),
      Seq(JsTransportAddress("Server 3"), JsTransportAddress("Server 4"))
    ),
    aggregatorAddress = JsTransportAddress("Aggregator"),
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2")
    ),
    leaderElectionAddresses = Seq(
      JsTransportAddress("LeaderElection 1"),
      JsTransportAddress("LeaderElection 2")
    ),
    acceptorAddresses = Seq(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
    ),
    replicaAddresses = Seq(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2")
    )
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

  // Servers.
  val servers = for (shard <- config.serverAddresses; address <- shard) yield {
    new Server[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ServerOptions.default.copy(
        pushPeriod = java.time.Duration.ofSeconds(10),
        recoverPeriod = java.time.Duration.ofSeconds(30),
        logGrowSize = 5
      ),
      metrics = new ServerMetrics(FakeCollectors)
    )
  }
  val server1 = servers(0)
  val server2 = servers(1)
  val server3 = servers(2)
  val server4 = servers(3)

  // Aggregator.
  val aggregator = new Aggregator[JsTransport](
    address = config.aggregatorAddress,
    transport = transport,
    logger = new JsLogger(),
    config = config,
    options = AggregatorOptions.default.copy(
      numShardCutsPerProposal = 4,
      recoverPeriod = java.time.Duration.ofSeconds(30),
      leaderInfoPeriod = java.time.Duration.ofSeconds(6),
      logGrowSize = 5
    ),
    metrics = new AggregatorMetrics(FakeCollectors)
  )

  // Leaders.
  val leaders = for (address <- config.leaderAddresses) yield {
    new Leader[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = LeaderOptions.default.copy(
        resendPhase1asPeriod = java.time.Duration.ofSeconds(10),
        flushPhase2asEveryN = 1,
        electionOptions = ElectionOptions.default.copy(
          pingPeriod = java.time.Duration.ofSeconds(60),
          noPingTimeoutMin = java.time.Duration.ofSeconds(120),
          noPingTimeoutMax = java.time.Duration.ofSeconds(240)
        ),
        logGrowSize = 5
      ),
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }
  val leader1 = leaders(0)
  val leader2 = leaders(1)

  // Acceptors.
  val acceptors = for (address <- config.acceptorAddresses)
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
  val acceptor1 = acceptors(0)
  val acceptor2 = acceptors(1)
  val acceptor3 = acceptors(2)

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
        batchFlush = true,
        recoverLogEntryMinPeriod = java.time.Duration.ofSeconds(60),
        recoverLogEntryMaxPeriod = java.time.Duration.ofSeconds(120)
      ),
      metrics = new ReplicaMetrics(FakeCollectors)
    )
  }
  val replica1 = replicas(0)
  val replica2 = replicas(1)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.scalog.Scalog")
object Scalog {
  val Scalog = new Scalog(batch = false, flexible = false);
}
