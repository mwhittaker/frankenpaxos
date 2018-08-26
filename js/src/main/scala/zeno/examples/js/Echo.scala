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
    new EchoServerActor[JsTransport](serverAddress, transport, logger);

  // Clients.
  val clientLoggers = mutable.Buffer[JsLogger]();
  val clients = mutable.Buffer[JsLogger]();
  for (name <- Seq("a", "b", "c")) {
    val clientLogger = new JsLogger()
    clientLoggers += clientLogger
    clients += new EchoClientActor[JsTransport](
      JsTransportAddress(s"client $name"),
      serverAddress,
      transport,
      clientLogger
    );
  }
}
