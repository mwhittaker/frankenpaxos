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
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer
}

@JSExportAll
case class LeaderOptions(
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override val serializer = LeaderInboundSerializer

  // Fields ////////////////////////////////////////////////////////////////////

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
    import LeaderInbound.Request

    val label =
      inbound.request match {
        case Request.Phase1B(_)            => "Phase1b"
        case Request.ClientRequest(_)      => "ClientRequest"
        case Request.ClientRequestBatch(_) => "ClientRequestBatch"
        case Request.Nack(_)               => "Nack"
        case Request.ChosenWatermark(_)    => "ChosenWatermark"
        case Request.Recover(_)            => "Recover"
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1B(r)            => handlePhase1b(src, r)
        case Request.ClientRequest(r)      => handleClientRequest(src, r)
        case Request.ClientRequestBatch(r) => handleClientRequestBatch(src, r)
        case Request.Nack(r)               => handleNack(src, r)
        case Request.ChosenWatermark(r)    => handleChosenWatermark(src, r)
        case Request.Recover(r)            => handleRecover(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    // TODO(mwhittaker): Implement
    ???
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    // TODO(mwhittaker): Implement
    ???
  }

  private def handleClientRequestBatch(
      src: Transport#Address,
      clientRequestBatch: ClientRequestBatch
  ): Unit = {
    // TODO(mwhittaker): Implement
    ???
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    // TODO(mwhittaker): Implement
    ???
  }

  private def handleChosenWatermark(
      src: Transport#Address,
      chosenWatermark: ChosenWatermark
  ): Unit = {
    // TODO(mwhittaker): Implement
    ???
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    // TODO(mwhittaker): Implement
    ???
  }
}
