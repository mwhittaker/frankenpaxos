package frankenpaxos.scalog

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
import scala.util.Random

@JSExportAll
object ProxyReplicaInboundSerializer
    extends ProtoSerializer[ProxyReplicaInbound] {
  type A = ProxyReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object ProxyReplica {
  val serializer = ProxyReplicaInboundSerializer
}

@JSExportAll
case class ProxyReplicaOptions(
    // If batchFlush is true, then the proxy replica flushes all client
    // channels after every batch of commands is processed.
    batchFlush: Boolean,
    measureLatencies: Boolean
)

@JSExportAll
object ProxyReplicaOptions {
  val default = ProxyReplicaOptions(
    batchFlush = false,
    measureLatencies = true
  )
}

@JSExportAll
class ProxyReplicaMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("scalog_proxy_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("scalog_proxy_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class ProxyReplica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ProxyReplicaOptions = ProxyReplicaOptions.default,
    metrics: ProxyReplicaMetrics = new ProxyReplicaMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ProxyReplicaInbound
  override val serializer = ProxyReplicaInboundSerializer

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

  private def clientChan(commandId: CommandId): Chan[Client[Transport]] = {
    val clientAddress = transport.addressSerializer.fromBytes(
      commandId.clientAddress.toByteArray()
    )
    clients.getOrElseUpdate(
      clientAddress,
      chan[Client[Transport]](clientAddress, Client.serializer)
    )
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ProxyReplicaInbound.Request

    val label =
      inbound.request match {
        case Request.ClientReplyBatch(_) => "ClientReplyBatch"
        case Request.Empty =>
          logger.fatal("Empty ProxyReplicaInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientReplyBatch(r) => handleClientReplyBatch(src, r)
        case Request.Empty =>
          logger.fatal("Empty ProxyReplicaInbound encountered.")
      }
    }
  }

  private def handleClientReplyBatch(
      src: Transport#Address,
      clientReplyBatch: ClientReplyBatch
  ): Unit = {
    for (clientReply <- clientReplyBatch.batch) {
      val client = clientChan(clientReply.commandId)

      if (options.batchFlush) {
        client.sendNoFlush(ClientInbound().withClientReply(clientReply))
      } else {
        client.send(ClientInbound().withClientReply(clientReply))
      }
    }

    if (options.batchFlush) {
      clients.values.foreach(_.flush())
    }
  }
}
