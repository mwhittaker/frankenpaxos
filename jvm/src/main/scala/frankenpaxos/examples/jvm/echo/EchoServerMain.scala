package frankenpaxos.echo.jvm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import frankenpaxos.Actor;
import frankenpaxos.NettyTcpAddress;
import frankenpaxos.NettyTcpTransport;
import frankenpaxos.PrintLogger;
import frankenpaxos.ScalaLoggingLogger;
import frankenpaxos.echo.EchoServerActor;

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
