package zeno.examples

import java.net.InetAddress;
import java.net.InetSocketAddress;
import scala.scalajs.js.annotation._;
import zeno.Actor;
import zeno.NettyTcpAddress
import zeno.NettyTcpTransport
import zeno.PrintLogger;
import zeno.ScalaLoggingLogger

@JSExportAll
class EchoClientActor[Transport <: zeno.Transport[Transport]](
    srcAddress: Transport#Address,
    dstAddress: Transport#Address,
    transport: Transport
) extends Actor(srcAddress, transport) {
  println(s"Echo client listening on $srcAddress.")
  var pingTimer: Transport#Timer =
    timer("pingTimer", java.time.Duration.ofSeconds(1), () => {
      send(dstAddress, EchoRequest(msg = "ping").toByteArray);
      pingTimer.start()
    });
  pingTimer.start();

  override def html(): String = { "" }

  override def receive(src: Transport#Address, bytes: Array[Byte]): Unit = {
    val reply = EchoReply.parseFrom(bytes);
    println(s"[$srcAddress] Received ${reply.msg} from $src.");
  }

  def echo(msg: String): Unit = {
    send(dstAddress, EchoRequest(msg = msg).toByteArray);
  }
}
