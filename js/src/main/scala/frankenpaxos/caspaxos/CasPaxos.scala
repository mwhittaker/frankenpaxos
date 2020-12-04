package frankenpaxos.caspaxos

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.monitoring.FakeCollectors
import scala.scalajs.js.annotation._

@JSExportAll
class CasPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2"),
      JsTransportAddress("Leader 3")
    ),
    acceptorAddresses = Seq(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
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
        resendClientRequestTimerPeriod = java.time.Duration.ofSeconds(10)
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
        resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(3),
        resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(3),
        minNackSleepPeriod = java.time.Duration.ofSeconds(1),
        maxNackSleepPeriod = java.time.Duration.ofSeconds(5)
      ),
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }
  val leader1 = leaders(0)
  val leader2 = leaders(1)
  val leader3 = leaders(2)

  // Acceptors.
  val acceptors = for (i <- 1 to config.acceptorAddresses.size) yield {
    new Acceptor[JsTransport](
      address = JsTransportAddress(s"Acceptor $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = AcceptorOptions.default.copy(),
      metrics = new AcceptorMetrics(FakeCollectors)
    )
  }
  val acceptor1 = acceptors(0)
  val acceptor2 = acceptors(1)
  val acceptor3 = acceptors(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.caspaxos.CasPaxos")
object CasPaxos {
  val CasPaxos = new CasPaxos();
}

@JSExportAll
@JSExportTopLevel("foobar")
object Foo {
  val bar = 1
}
