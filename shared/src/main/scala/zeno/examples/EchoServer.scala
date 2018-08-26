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
  var numMessagesReceived: Int = 0

  println(s"Echo server listening on $address.")

  override def receive(src: Transport#Address, bytes: Array[Byte]): Unit = {
    numMessagesReceived += 1
    val request = EchoRequest.parseFrom(bytes)
    logger.info(s"[$address] Received ${request.msg} from $src.")
    send(src, request.toByteArray)
  }
}
