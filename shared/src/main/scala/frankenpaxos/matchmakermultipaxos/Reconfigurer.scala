package frankenpaxos.matchmakermultipaxos

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.election.basic.Participant
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.quorums.QuorumSystem
import frankenpaxos.quorums.QuorumSystemProto
import frankenpaxos.quorums.SimpleMajority
import frankenpaxos.quorums.UnanimousWrites
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object ReconfigurerInboundSerializer
    extends ProtoSerializer[ReconfigurerInbound] {
  type A = ReconfigurerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Reconfigurer {
  val serializer = ReconfigurerInboundSerializer
}

@JSExportAll
case class ReconfigurerOptions(
    measureLatencies: Boolean
)

@JSExportAll
object ReconfigurerOptions {
  val default = ReconfigurerOptions(
    measureLatencies = true
  )
}

@JSExportAll
class ReconfigurerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_reconfigurer_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakermultipaxos_reconfigurer_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class Reconfigurer[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ReconfigurerOptions = ReconfigurerOptions.default,
    metrics: ReconfigurerMetrics = new ReconfigurerMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.reconfigurerAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReconfigurerInbound
  override val serializer = ReconfigurerInboundSerializer

  // Fields ////////////////////////////////////////////////////////////////////

  // Timers ////////////////////////////////////////////////////////////////////

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
    import ReconfigurerInbound.Request

    val label =
      inbound.request match {
        case Request.Reconfigure(_)  => "Reconfigure"
        case Request.StopAck(_)      => "StopAck"
        case Request.BootstrapAck(_) => "BootstrapAck"
        case Request.MatchPhase1B(_) => "MatchPhase1b"
        case Request.MatchPhase2B(_) => "MatchPhase2b"
        case Request.MatchChosen(_)  => "MatchChosen"
        case Request.Empty =>
          logger.fatal("Empty ReconfigurerInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Reconfigure(r)  => handleReconfigure(src, r)
        case Request.StopAck(r)      => handleStopAck(src, r)
        case Request.BootstrapAck(r) => handleBootstrapAck(src, r)
        case Request.MatchPhase1B(r) => handleMatchPhase1b(src, r)
        case Request.MatchPhase2B(r) => handleMatchPhase2b(src, r)
        case Request.MatchChosen(r)  => handleMatchChosen(src, r)
        case Request.Empty =>
          logger.fatal("Empty ReconfigurerInbound encountered.")
      }
    }
  }

  private def handleReconfigure(
      src: Transport#Address,
      reconfigure: Reconfigure
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleStopAck(src: Transport#Address, stopAck: StopAck): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleBootstrapAck(
      src: Transport#Address,
      bootstrapAck: BootstrapAck
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleMatchPhase1b(
      src: Transport#Address,
      matchPhase1b: MatchPhase1b
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleMatchPhase2b(
      src: Transport#Address,
      matchPhase2b: MatchPhase2b
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleMatchChosen(
      src: Transport#Address,
      matchChosen: MatchChosen
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }
}
