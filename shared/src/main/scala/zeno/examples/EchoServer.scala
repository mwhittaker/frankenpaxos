package zeno.examples;

import java.net.InetAddress
import java.net.InetSocketAddress
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer

@JSExportAll
object EchoRequestSerializer extends ProtoSerializer[EchoRequest] {
  type A = EchoRequest
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

object EchoServerActor {
  val serializer = EchoRequestSerializer
}

@JSExportAll
class EchoServerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = EchoRequest
  override def serializer = EchoServerActor.serializer

  var numMessagesReceived: Int = 0

  println(s"Echo server listening on $address.")

  override def receive(src: Transport#Address, request: EchoRequest): Unit = {
    logger.info(s"Received ${request.msg} from $src.")
    numMessagesReceived += 1

    val client =
      typedActorClient[EchoClientActor[Transport]](
        src,
        EchoClientActor.serializer
      );
    client.send(EchoReply(msg = request.msg))
  }
}
