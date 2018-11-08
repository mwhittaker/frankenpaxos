package zeno.examples

import java.net.InetAddress
import java.net.InetSocketAddress
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.NettyTcpAddress
import zeno.NettyTcpTransport

@JSExportAll
class EchoClientActor[Transport <: zeno.Transport[Transport]](
    srcAddress: Transport#Address,
    dstAddress: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(srcAddress, transport, logger) {
  type InboundMessage = EchoReply;

  var numMessagesReceived: Int = 0

  private val pingTimer: Transport#Timer =
    timer("pingTimer", java.time.Duration.ofSeconds(1), () => {
      send(dstAddress, EchoRequest(msg = "ping").toByteArray);
      pingTimer.start()
    });

  println(s"Echo client listening on $srcAddress.")
  pingTimer.start();

  override def parseInboundMessage(bytes: Array[Byte]): InboundMessage = {
    EchoReply.parseFrom(bytes)
  }

  override def parseInboundMessageToString(bytes: Array[Byte]): String = {
    parseInboundMessage(bytes).toProtoString
  }

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    numMessagesReceived += 1
    logger.info(s"Received ${reply.msg} from $src.")
  }

  def echo(msg: String): Unit = {
    send(dstAddress, EchoRequest(msg = msg).toByteArray)
  }
}
