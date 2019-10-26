package frankenpaxos.batchedunreplicated

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._

@JSExportAll
object ProxyServerInboundSerializer
    extends ProtoSerializer[ProxyServerInbound] {
  type A = ProxyServerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object ProxyServer {
  val serializer = ProxyServerInboundSerializer
}

@JSExportAll
case class ProxyServerOptions(
    measureLatencies: Boolean
)

@JSExportAll
object ProxyServerOptions {
  val default = ProxyServerOptions(
    measureLatencies = true
  )
}

@JSExportAll
class ProxyServerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("batchedunreplicated_proxy_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("batchedunreplicated_proxy_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class ProxyServer[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ProxyServerOptions = ProxyServerOptions.default,
    metrics: ProxyServerMetrics = new ProxyServerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ProxyServerInbound
  override val serializer = ProxyServerInboundSerializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Client channels.
  @JSExport
  protected val clients =
    mutable.Map[Transport#Address, Chan[Client[Transport]]]()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def timed[T](label: String)(e: => T): T = {
    if (options.measureLatencies) {
      val startNanos = System.nanoTime
      val x = e
      val stopNanos = System.nanoTime
      metrics.requestsLatency
        .labels(label)
        .observe((stopNanos - startNanos).toDouble / 1000000)
      x
    } else {
      e
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ProxyServerInbound.Request

    val label =
      inbound.request match {
        case Request.ClientReplyBatch(_) => "ClientReplyBatch"
        case Request.Empty =>
          logger.fatal("Empty ProxyServerInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientReplyBatch(r) => handleClientReplyBatch(src, r)
        case Request.Empty =>
          logger.fatal("Empty ProxyServerInbound encountered.")
      }
    }
  }

  private def handleClientReplyBatch(
      src: Transport#Address,
      clientReplyBatch: ClientReplyBatch
  ): Unit = {
    for (result <- clientReplyBatch.result) {
      val clientAddress = transport.addressSerializer.fromBytes(
        result.clientAddress.toByteArray()
      )
      val client = chan[Client[Transport]](clientAddress, Client.serializer)
      client.send(ClientInbound().withClientReply(ClientReply(result = result)))
    }
  }
}
