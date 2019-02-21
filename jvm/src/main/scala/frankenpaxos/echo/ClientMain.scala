package frankenpaxos.echo

import java.net.InetAddress
import java.net.InetSocketAddress
import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.ScalaLoggingLogger

object ClientMain {
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
      new Client[NettyTcpTransport](srcAddress, dstAddress, transport, logger)
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
