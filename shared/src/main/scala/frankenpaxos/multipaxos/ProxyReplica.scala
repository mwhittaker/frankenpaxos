package frankenpaxos.multipaxos

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
    measureLatencies: Boolean
)

@JSExportAll
object ProxyReplicaOptions {
  val default = ProxyReplicaOptions(
    measureLatencies = true
  )
}

@JSExportAll
class ProxyReplicaMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_proxy_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_proxy_replica_requests_latency")
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
  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (address <- config.leaderAddresses)
      yield chan[Leader[Transport]](address, Leader.serializer)

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
    import ProxyReplicaInbound.Request

    val label =
      inbound.request match {
        case Request.ClientReplyBatch(_) => "ClientReplyBatch"
        case Request.ChosenWatermark(_)  => "ChosenWatermark"
        case Request.Recover(_)          => "Recover"
        case Request.Empty =>
          logger.fatal("Empty ProxyReplicaInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientReplyBatch(r) => handleClientReplyBatch(src, r)
        case Request.ChosenWatermark(r)  => handleChosenWatermark(src, r)
        case Request.Recover(r)          => handleRecover(src, r)
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
      val client = chan[Client[Transport]](
        transport.addressSerializer
          .fromBytes(clientReply.commandId.clientAddress.toByteArray()),
        Client.serializer
      )
      client.send(ClientInbound().withClientReply(clientReply))
    }
  }

  private def handleChosenWatermark(
      src: Transport#Address,
      chosenWatermark: ChosenWatermark
  ): Unit = {
    leaders.foreach(
      _.send(LeaderInbound().withChosenWatermark(chosenWatermark))
    )
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    leaders.foreach(_.send(LeaderInbound().withRecover(recover)))
  }
}
