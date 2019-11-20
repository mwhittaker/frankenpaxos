package frankenpaxos.matchmakerpaxos

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
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object MatchmakerInboundSerializer extends ProtoSerializer[MatchmakerInbound] {
  type A = MatchmakerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Matchmaker {
  val serializer = MatchmakerInboundSerializer
}

@JSExportAll
case class MatchmakerOptions(
    measureLatencies: Boolean
)

@JSExportAll
object MatchmakerOptions {
  val default = MatchmakerOptions(
    measureLatencies = true
  )
}

@JSExportAll
class MatchmakerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakerpaxos_matchmaker_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakerpaxos_matchmaker_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class Matchmaker[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: MatchmakerOptions = MatchmakerOptions.default,
    metrics: MatchmakerMetrics = new MatchmakerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.matchmakerAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = MatchmakerInbound
  override val serializer = MatchmakerInboundSerializer
  type Round = Int

  // Fields ////////////////////////////////////////////////////////////////////
  private val index = config.matchmakerAddresses.indexOf(address)

  @JSExport
  protected val acceptorGroups = mutable.SortedMap[Round, AcceptorGroup]()

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
    import MatchmakerInbound.Request

    val label = inbound.request match {
      case Request.MatchRequest(_) =>
        "MatchRequest"
      case Request.Empty =>
        logger.fatal("Empty MatchmakerInbound encountered.")
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.MatchRequest(r) =>
          handleMatchRequest(src, r)
        case Request.Empty =>
          logger.fatal("Empty MatchmakerInbound encountered.")
      }
    }
  }

  private def handleMatchRequest(
      src: Transport#Address,
      matchRequest: MatchRequest
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // A matchmaker only processes a match request if the request's round is
    // larger than any previously seen. Otherwise, a nack is sent back.
    //
    // It's possible to implement things so that leaders re-send MatchRequests
    // if they haven't received a MatchReply for a while. If we did this, then
    // the matchmaker would have to re-send replies to requests that it has
    // already processed. We don't do this though. Client re-sends address this
    // liveness issue, so matchmakers are free to ignore this request.
    if (matchRequest.acceptorGroup.round <= acceptorGroups.lastKey) {
      logger.debug(
        s"Matchmaker received a MatchRequest in round " +
          s"${matchRequest.acceptorGroup.round} but has already processed a " +
          s"MatchRequest in round ${acceptorGroups.lastKey}, so the request " +
          s"is being ignored."
      )
      leader.send(
        LeaderInbound()
          .withMatchmakerNack(MatchmakerNack(round = acceptorGroups.lastKey))
      )
      return
    }

    // Send back all previous acceptor groups and store the new acceptor group.
    leader.send(
      LeaderInbound().withMatchReply(
        MatchReply(
          round = matchRequest.acceptorGroup.round,
          matchmakerIndex = index,
          acceptorGroup = acceptorGroups.values.toSeq
        )
      )
    )
    acceptorGroups(matchRequest.acceptorGroup.round) =
      matchRequest.acceptorGroup
  }
}
