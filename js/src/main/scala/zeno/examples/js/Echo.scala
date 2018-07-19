package zeno.examples.js

import scala.scalajs.js.annotation._;
import zeno.Actor;
import zeno.FakeTransport;
import zeno.FakeTransportAddress;
import zeno.PrintLogger;
import zeno.ScalaLoggingLogger;
import zeno.examples.EchoClientActor;
import zeno.examples.EchoServerActor;

@JSExportAll
@JSExportTopLevel("zeno.examples.js.Echo")
object Echo {
  val logger = new PrintLogger()
  val transport = new FakeTransport(logger);
  val clientAddress = FakeTransportAddress("client")
  val serverAddress = FakeTransportAddress("server")
  val chatClient =
    new EchoClientActor[FakeTransport](clientAddress, serverAddress, transport);
  val chatServer = new EchoServerActor[FakeTransport](serverAddress, transport);
}
