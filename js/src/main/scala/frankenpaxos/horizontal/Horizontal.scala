package frankenpaxos.horizontal

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class Horizontal {
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
    acceptorAddresses = Seq(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3"),
      JsTransportAddress("Acceptor 4"),
      JsTransportAddress("Acceptor 5"),
      JsTransportAddress("Acceptor 6"),
      JsTransportAddress("Acceptor 7")
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
        logGrowSize = 6,
        alpha = 3,
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
  val acceptor7 = acceptors(6)

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
@JSExportTopLevel("frankenpaxos.horizontal.Horizontal")
object Horizontal {
  val Horizontal = new Horizontal();
}
