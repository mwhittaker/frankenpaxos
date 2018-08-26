package zeno.examples.js

import scala.collection.mutable
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.JsLogger
import zeno.JsTransport
import zeno.JsTransportAddress
import zeno.examples.EchoClientActor
import zeno.examples.EchoServerActor

@JSExportAll
@JSExportTopLevel("zeno.examples.js.Echo")
object Echo {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger);

  // Server.
  val serverAddress = JsTransportAddress("server")
  val serverLogger = new JsLogger()
  val server =
    new EchoServerActor[JsTransport](serverAddress, transport, serverLogger);

  // Clients.
  val clientA = new EchoClientActor[JsTransport](
    new JsTransportAddress("client a"),
    serverAddress,
    transport,
    new JsLogger()
  );

  val clientB = new EchoClientActor[JsTransport](
    new JsTransportAddress("client b"),
    serverAddress,
    transport,
    new JsLogger()
  );
}
