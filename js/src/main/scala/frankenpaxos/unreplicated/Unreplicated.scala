package frankenpaxos.unreplicated

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class Unreplicated {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    new Client[JsTransport](
      address = JsTransportAddress(s"Client $i"),
      transport = transport,
      logger = new JsLogger(),
      serverAddress = JsTransportAddress(s"Server"),
      options = ClientOptions.default,
      metrics = new ClientMetrics(FakeCollectors)
    )
  }
  val client1 = clients(0)
  val client2 = clients(1)
  val client3 = clients(2)

  // Server
  val server =
    new Server[JsTransport](
      address = JsTransportAddress(s"Server"),
      transport = transport,
      logger = new JsLogger(),
      stateMachine = new AppendLog(),
      options = ServerOptions.default,
      metrics = new ServerMetrics(FakeCollectors)
    )
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.unreplicated.Unreplicated")
object Unreplicated {
  val Unreplicated = new Unreplicated();
}
