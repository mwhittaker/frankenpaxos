package zeno.examples.jvm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import zeno.Actor;
import zeno.NettyTcpAddress;
import zeno.NettyTcpTransport;
import zeno.PrintLogger;
import zeno.ScalaLoggingLogger;
import zeno.examples.EchoServerActor;

object EchoServerMain {
  def main(args: Array[String]): Unit = {
    val logger = new PrintLogger()
    val transport = new NettyTcpTransport(logger);
    val address = NettyTcpAddress(
      new InetSocketAddress(InetAddress.getLocalHost(), 9000)
    );
    val chatServer =
      new EchoServerActor[NettyTcpTransport](address, transport, logger);
  }
}
