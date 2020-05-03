package frankenpaxos.fasterpaxos

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.heartbeat.HeartbeatOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class FasterPaxos(f: Int) {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = f,
    serverAddresses =
      (1 to 2 * f + 1).map(i => JsTransportAddress(s"Server $i")),
    heartbeatAddresses =
      (1 to 2 * f + 1).map(i => JsTransportAddress(s"Heartbeat $i"))
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
  val servers = for (address <- config.serverAddresses) yield {
    new Server[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      stateMachine = new AppendLog(),
      config = config,
      options = ServerOptions.default.copy(
        ackNoopsWithCommands = true,
        logGrowSize = 5,
        resendPhase1asPeriod = java.time.Duration.ofSeconds(10),
        resendPhase2aAnysPeriod = java.time.Duration.ofSeconds(10),
        useF1Optimization = true,
        heartbeatOptions = HeartbeatOptions(
          failPeriod = java.time.Duration.ofSeconds(5),
          successPeriod = java.time.Duration.ofSeconds(30),
          numRetries = 1,
          networkDelayAlpha = 0.9
        )
      ),
      metrics = new ServerMetrics(FakeCollectors)
    )
  }
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.fasterpaxos.FasterPaxosF1")
object FasterPaxosF1 {
  val FasterPaxos = new FasterPaxos(f = 1);
  val server1 = FasterPaxos.servers(0)
  val server2 = FasterPaxos.servers(1)
  val server3 = FasterPaxos.servers(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.fasterpaxos.FasterPaxosF2")
object FasterPaxosF2 {
  val FasterPaxos = new FasterPaxos(f = 2);
  val server1 = FasterPaxos.servers(0)
  val server2 = FasterPaxos.servers(1)
  val server3 = FasterPaxos.servers(2)
  val server4 = FasterPaxos.servers(3)
  val server5 = FasterPaxos.servers(4)
}
