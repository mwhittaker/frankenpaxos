package frankenpaxos.echo.js

import scala.collection.mutable
import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.echo.EchoClientActor
import frankenpaxos.echo.EchoServerActor

@JSExportAll
class Echo {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger);

  // Server.
  val serverAddress = JsTransportAddress("Server")
  val serverLogger = new JsLogger()
  val server =
    new EchoServerActor[JsTransport](serverAddress, transport, serverLogger);

  // Clients.
  val clientA = new EchoClientActor[JsTransport](
    new JsTransportAddress("Client A"),
    serverAddress,
    transport,
    new JsLogger()
  );

  val clientB = new EchoClientActor[JsTransport](
    new JsTransportAddress("Client B"),
    serverAddress,
    transport,
    new JsLogger()
  );
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.echo.js.SimulatedEcho")
object SimulatedEcho {
  val Echo = new Echo();
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.echo.js.ClickthroughEcho")
object ClickthroughEcho {
  val Echo = new Echo();
}
