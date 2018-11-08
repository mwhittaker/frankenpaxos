package zeno.examples;

import java.net.InetAddress
import java.net.InetSocketAddress
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger

@JSExportAll
class EchoServerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {
  type InboundMessage = EchoRequest;

  var numMessagesReceived: Int = 0

  println(s"Echo server listening on $address.")

  override def parseInboundMessage(bytes: Array[Byte]): InboundMessage = {
    EchoRequest.parseFrom(bytes)
  }

  override def parseInboundMessageToString(bytes: Array[Byte]): String = {
    parseInboundMessage(bytes).toProtoString
  }

  override def receive(src: Transport#Address, request: EchoRequest): Unit = {
    numMessagesReceived += 1
    logger.info(s"Received ${request.msg} from $src.")
    send(src, EchoReply(msg = request.msg).toByteArray)
  }
}
