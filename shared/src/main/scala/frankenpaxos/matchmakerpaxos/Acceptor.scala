package frankenpaxos.matchmakerpaxos

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
object AcceptorInboundSerializer extends ProtoSerializer[AcceptorInbound] {
  type A = AcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Acceptor {
  val serializer = AcceptorInboundSerializer
}

@JSExportAll
case class AcceptorOptions(
    measureLatencies: Boolean
)

@JSExportAll
object AcceptorOptions {
  val default = AcceptorOptions(
    measureLatencies = true
  )
}

@JSExportAll
class AcceptorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakerpaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakerpaxos_acceptor_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class Acceptor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: AcceptorOptions = AcceptorOptions.default,
    metrics: AcceptorMetrics = new AcceptorMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = AcceptorInbound
  override val serializer = AcceptorInboundSerializer
  type Round = Int

  // Fields ////////////////////////////////////////////////////////////////////
  private val index = config.acceptorAddresses.indexOf(address)

  @JSExport
  protected var round: Round = -1

  @JSExport
  protected var voteRound: Round = -1

  @JSExport
  protected var voteValue: Option[String] = None

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
    import AcceptorInbound.Request

    val label =
      inbound.request match {
        case Request.Phase1A(_) => "Phase1a"
        case Request.Phase2A(_) => "Phase2a"
        case Request.Empty =>
          logger.fatal("Empty AcceptorInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1A(r) => handlePhase1a(src, r)
        case Request.Phase2A(r) => handlePhase2a(src, r)
        case Request.Empty =>
          logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  private def handlePhase1a(
      src: Transport#Address,
      phase1a: Phase1a
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // If we receive an out of date round, we send back a nack.
    if (phase1a.round < round) {
      logger.debug(
        s"Acceptor received a Phase1a message in round ${phase1a.round} " +
          s"but is in round $round. The acceptor is sending back a nack."
      )
      leader.send(LeaderInbound().withAcceptorNack(AcceptorNack(round = round)))
      return
    }

    // Otherwise, we update our round and send back a Phase1b message to the
    // leader.
    round = phase1a.round
    val phase1b = Phase1b(
      round = phase1a.round,
      acceptorIndex = index,
      vote =
        voteValue.map(v => Phase1bVote(voteRound = voteRound, voteValue = v))
    )
    leader.send(LeaderInbound().withPhase1B(phase1b))
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // If we receive an out of date round, we send back a nack to the leader.
    if (phase2a.round < round) {
      logger.debug(
        s"Acceptor received a Phase2a message in round ${phase2a.round} " +
          s"but is in round $round. The acceptor is sending back a nack."
      )
      leader.send(LeaderInbound().withAcceptorNack(AcceptorNack(round = round)))
      return
    }

    // Otherwise, update our round and send back a Phase2b to the leader.
    round = phase2a.round
    voteRound = phase2a.round
    voteValue = Some(phase2a.value)
    leader.send(
      LeaderInbound()
        .withPhase2B(Phase2b(round = phase2a.round, acceptorIndex = index))
    )
  }
}
