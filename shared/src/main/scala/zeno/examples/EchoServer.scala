package zeno.examples;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import zeno.Actor;
import zeno.NettyTcpAddress;
import zeno.NettyTcpTransport;
import zeno.PrintLogger;
import zeno.ScalaLoggingLogger;

class EchoServerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport
) extends Actor(address, transport) {
  println(s"Echo server listening on $address.")

  override def html(): String = { "" }

  override def receive(src: Transport#Address, bytes: Array[Byte]): Unit = {
    val request = EchoRequest.parseFrom(bytes);
    println(s"Received ${request.msg} from $src.");
    send(src, request.toByteArray);
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
