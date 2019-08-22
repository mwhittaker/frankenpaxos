package frankenpaxos.caspaxos

import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object ClientInboundSerializer extends ProtoSerializer[ClientInbound] {
  type A = ClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Client {
  val serializer = ClientInboundSerializer
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = ClientInbound
  override def serializer = Client.serializer

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ClientInbound
  ): Unit = {
    import ClientInbound.Request
    inbound.request match {
      case Request.ClientReply(r) => handleClientReply(src, r)
      case Request.Empty => {
        logger.fatal("Empty ClientInbound encountered.")
      }
    }
  }

  private def handleClientReply(
      src: Transport#Address,
      clientRequest: ClientReply
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }
}
