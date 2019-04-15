package frankenpaxos.echo

import frankenpaxos.Actor
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.monitoring.FakeCollectors
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class Echo {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Server.
  val serverAddress = JsTransportAddress("Server")
  val serverLogger = new JsLogger()
  val server = new Server[JsTransport](serverAddress,
                                       transport,
                                       serverLogger,
                                       new ServerMetrics(FakeCollectors))

  // Clients.
  val clientA = new Client[JsTransport](
    new JsTransportAddress("Client A"),
    serverAddress,
    transport,
    new JsLogger()
  )

  val clientB = new Client[JsTransport](
    new JsTransportAddress("Client B"),
    serverAddress,
    transport,
    new JsLogger()
  )
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.echo.TweenedEcho")
object TweenedEcho {
  val Echo = new Echo()
}
