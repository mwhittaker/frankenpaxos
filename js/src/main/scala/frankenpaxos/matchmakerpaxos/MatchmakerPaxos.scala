package frankenpaxos.matchmakerpaxos

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class MatchmakerPaxos {
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
    matchmakerAddresses = Seq(
      JsTransportAddress("Matchmaker 1"),
      JsTransportAddress("Matchmaker 2"),
      JsTransportAddress("Matchmaker 3")
    ),
    acceptorAddresses = Seq(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3"),
      JsTransportAddress("Acceptor 4"),
      JsTransportAddress("Acceptor 5"),
      JsTransportAddress("Acceptor 6")
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
      options = LeaderOptions.default,
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }
  val leader1 = leaders(0)
  val leader2 = leaders(1)

  // Matchmakers.
  val matchmakers = for (address <- config.matchmakerAddresses) yield {
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
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.matchmakerpaxos.MatchmakerPaxos")
object MatchmakerPaxos {
  val MatchmakerPaxos = new MatchmakerPaxos();
}
