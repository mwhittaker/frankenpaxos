package zeno.examples;

import java.net.InetAddress
import java.net.InetSocketAddress
import zeno.Actor
import zeno.Logger

class EchoServerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(address, transport) {
  println(s"Echo server listening on $address.")

  override def html(): String = { "" }

  override def receive(src: Transport#Address, bytes: Array[Byte]): Unit = {
    val request = EchoRequest.parseFrom(bytes);
    logger.info(s"[$address] Received ${request.msg} from $src.");
    send(src, request.toByteArray);
  }
}
