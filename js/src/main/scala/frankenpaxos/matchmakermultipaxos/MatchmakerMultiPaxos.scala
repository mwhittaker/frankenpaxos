package frankenpaxos.matchmakermultipaxos

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class MatchmakerMultiPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2")
    ),
    leaderElectionAddresses = Seq(
      JsTransportAddress("LeaderElection 1"),
      JsTransportAddress("LeaderElection 2")
    ),
    reconfigurerAddresses = Seq(
      JsTransportAddress("Reconfigurer 1"),
      JsTransportAddress("Reconfigurer 2")
    ),
    matchmakerAddresses = Seq(
      JsTransportAddress("Matchmaker 1"),
      JsTransportAddress("Matchmaker 2"),
      JsTransportAddress("Matchmaker 3"),
      JsTransportAddress("Matchmaker 4"),
      JsTransportAddress("Matchmaker 5"),
      JsTransportAddress("Matchmaker 6")
    ),
    acceptorAddresses = Seq(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3"),
      JsTransportAddress("Acceptor 4"),
      JsTransportAddress("Acceptor 5"),
      JsTransportAddress("Acceptor 6")
    ),
    replicaAddresses = Seq(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2"),
      JsTransportAddress("Replica 3")
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
        resendClientRequestPeriod = java.time.Duration.ofSeconds(30)
      ),
      metrics = new ClientMetrics(FakeCollectors)
    )
  }
  val client1 = clients(0)
  val client2 = clients(1)
  val client3 = clients(2)

  // Leaders.
  val leaders = for (address <- config.leaderAddresses) yield {
    new Leader[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = LeaderOptions.default.copy(
        resendMatchRequestsPeriod = java.time.Duration.ofSeconds(10),
        resendPhase1asPeriod = java.time.Duration.ofSeconds(10),
        resendPhase2asPeriod = java.time.Duration.ofSeconds(10),
        sendChosenWatermarkEveryN = 2,
        stallDuringMatchmaking = false,
        stallDuringPhase1 = false,
        disableGc = false,
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

  // Reconfigurers.
  val reconfigurers = for (address <- config.reconfigurerAddresses)
    yield {
      new Reconfigurer[JsTransport](
        address = address,
        transport = transport,
        logger = new JsLogger(),
        config = config,
        options = ReconfigurerOptions.default,
        metrics = new ReconfigurerMetrics(FakeCollectors)
      )
    }
  val reconfigurer1 = reconfigurers(0)
  val reconfigurer2 = reconfigurers(1)

  // Matchmakers.
  val matchmakers = for (address <- config.matchmakerAddresses)
    yield {
      new Matchmaker[JsTransport](
        address = address,
        transport = transport,
        logger = new JsLogger(),
        config = config,
        options = MatchmakerOptions.default,
        metrics = new MatchmakerMetrics(FakeCollectors)
      )
    }
  val matchmaker1 = matchmakers(0)
  val matchmaker2 = matchmakers(1)
  val matchmaker3 = matchmakers(2)
  val matchmaker4 = matchmakers(3)
  val matchmaker5 = matchmakers(4)
  val matchmaker6 = matchmakers(5)

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
  val acceptor4 = acceptors(3)
  val acceptor5 = acceptors(4)
  val acceptor6 = acceptors(5)

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
        recoverLogEntryMinPeriod = java.time.Duration.ofSeconds(60),
        recoverLogEntryMaxPeriod = java.time.Duration.ofSeconds(120),
        unsafeDontRecover = false
      ),
      metrics = new ReplicaMetrics(FakeCollectors)
    )
  }
  val replica1 = replicas(0)
  val replica2 = replicas(1)
  val replica3 = replicas(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.matchmakermultipaxos.MatchmakerMultiPaxos")
object MatchmakerMultiPaxos {
  val MatchmakerMultiPaxos = new MatchmakerMultiPaxos();
}
