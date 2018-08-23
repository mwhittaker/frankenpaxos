package zeno.examples.js

import scala.scalajs.js.annotation._;
import zeno.Actor;
import zeno.JsTransport;
import zeno.JsTransportAddress;
import zeno.PrintLogger;
import zeno.ScalaLoggingLogger;
import zeno.examples.EchoClientActor;
import zeno.examples.EchoServerActor;

@JSExportAll
@JSExportTopLevel("zeno.examples.js.Echo")
object Echo {
  val logger = new PrintLogger()
  val transport = new JsTransport(logger);
  val serverAddress = JsTransportAddress("server")
  val clientA = new EchoClientActor[JsTransport](
    JsTransportAddress("client a"),
    serverAddress,
    transport
  );
  val clientB = new EchoClientActor[JsTransport](
    JsTransportAddress("client b"),
    serverAddress,
    transport
  );
  val clientC = new EchoClientActor[JsTransport](
    JsTransportAddress("client c"),
    serverAddress,
    transport
  );
  val server = new EchoServerActor[JsTransport](serverAddress, transport);
}
