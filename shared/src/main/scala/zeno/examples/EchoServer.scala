package zeno.examples

import java.net.InetAddress;
import java.net.InetSocketAddress;
import zeno.Actor;
import zeno.ScalaLoggingLogger
import zeno.PrintLogger;
import zeno.NettyTcpTransport
import zeno.NettyTcpAddress

class EchoServerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport
) extends Actor(address, transport) {
  println(s"Echo server listening on $address.")

  override def html(): String = { "" }

  override def receive(src: Transport#Address, msg: String): Unit = {
    println(s"Received $msg from $src.");
    send(src, msg);
  }
}

object EchoServerMain {
  def main(args: Array[String]): Unit = {
    val logger = new PrintLogger()
    val transport = new NettyTcpTransport(logger);
    val address = NettyTcpAddress(
      new InetSocketAddress(InetAddress.getLocalHost(), 9000)
    );
    val chatServer = new EchoServerActor[NettyTcpTransport](address, transport);
  }
}
