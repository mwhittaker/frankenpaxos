package zeno.examples

import java.net.InetAddress;
import java.net.InetSocketAddress;
import zeno.Actor;
import zeno.ScalaLoggingLogger
import zeno.PrintLogger;
import zeno.NettyTcpTransport
import zeno.NettyTcpAddress

class EchoClientActor[Transport <: zeno.Transport[Transport]](
    srcAddress: Transport#Address,
    dstAddress: Transport#Address,
    transport: Transport
) extends Actor(srcAddress, transport) {
  println(s"Echo client listening on $srcAddress.")
  var pingTimer: Transport#Timer =
    transport.timer("pingTimer", java.time.Duration.ofSeconds(1), () => {
      send(dstAddress, EchoRequest(msg = "ping").toByteArray);
      pingTimer.start()
    });
  pingTimer.start();

  override def html(): String = { "" }

  override def receive(src: Transport#Address, bytes: Array[Byte]): Unit = {
    val reply = EchoReply.parseFrom(bytes);
    println(s"Received $bytes from $src.");
  }

  def echo(msg: String): Unit = {
    send(dstAddress, EchoRequest(msg = msg).toByteArray);
  }
}

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
