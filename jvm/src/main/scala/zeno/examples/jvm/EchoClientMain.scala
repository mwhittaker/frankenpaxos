package zeno.examples.jvm

import java.net.InetAddress;
import java.net.InetSocketAddress;
import zeno.Actor;
import zeno.NettyTcpAddress;
import zeno.NettyTcpTransport;
import zeno.PrintLogger;
import zeno.ScalaLoggingLogger;
import zeno.examples.EchoClientActor;

object EchoClientMain {
  def main(args: Array[String]): Unit = {
    val logger = new PrintLogger()
    val transport = new NettyTcpTransport(logger);
    val srcAddress = NettyTcpAddress(
      new InetSocketAddress(InetAddress.getLocalHost(), 9001)
    );
    val dstAddress = NettyTcpAddress(
      new InetSocketAddress(InetAddress.getLocalHost(), 9000)
    );
    val chatClient =
      new EchoClientActor[NettyTcpTransport](srcAddress, dstAddress, transport);
    var ok = true
    while (ok) {
      val line = readLine()
      ok = line != null
      if (ok) {
        chatClient.echo(line)
      }
    }
  }
}
