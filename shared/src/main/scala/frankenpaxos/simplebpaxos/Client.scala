package frankenpaxos.simplebpaxos

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.PrometheusCollectors
import scala.scalajs.js.annotation._

@JSExportAll
object ClientInboundSerializer extends ProtoSerializer[ClientInbound] {
  type A = ClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class ClientOptions(
    // TODO(mwhittaker): Add options.
)

@JSExportAll
object ClientOptions {
  // TODO(mwhittaker): Add options.
  val default = ClientOptions()
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {
  // TODO(mwhittaker): Add metrics.
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
    // TODO(mwhittaker): Add config.
    // config: Config[Transport],
    // TODO(mwhittaker): Add state machine.
    // TODO(mwhittaker): Add dependency graph.
    options: ClientOptions = ClientOptions.default,
    metrics: ClientMetrics = new ClientMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Client._

  // Types /////////////////////////////////////////////////////////////////////
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
      clientReply: ClientReply
  ): Unit = ???
}
