package frankenpaxos.echo

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.scalajs.js.annotation._

@JSExportAll
object EchoRequestSerializer extends ProtoSerializer[ServerInbound] {
  type A = ServerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

object EchoServerActor {
  val serializer = EchoRequestSerializer
}

@JSExportAll
class EchoServerActor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = ServerInbound
  override def serializer = EchoServerActor.serializer

  var numMessagesReceived: Int = 0

  logger.info(s"Echo server listening on $address.")

  override def receive(src: Transport#Address, request: ServerInbound): Unit = {
    logger.info(s"Received ${request.msg} from $src.")
    numMessagesReceived += 1

    val client =
      typedActorClient[EchoClientActor[Transport]](
        src,
        EchoClientActor.serializer
      );
    client.send(ClientInbound(msg = request.msg))
  }
}
