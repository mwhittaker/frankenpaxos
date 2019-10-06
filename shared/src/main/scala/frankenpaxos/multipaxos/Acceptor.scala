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
    .name("multipaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_acceptor_requests_latency")
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

  type Slot = Int

  @JSExportAll
  case class State(
      voteRound: Int,
      voteValue: CommandBatchOrNoop
  )

  // Fields ////////////////////////////////////////////////////////////////////
  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (address <- config.leaderAddresses)
      yield chan[Leader[Transport]](address, Leader.serializer)

  // Acceptor index.
  private val groupIndex =
    config.acceptorAddresses.indexWhere(_.contains(address))
  private val index = config.acceptorAddresses(groupIndex).indexOf(address)

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  @JSExport
  protected var round: Int = -1

  @JSExport
  protected var states = mutable.SortedMap[Slot, State]()

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
        s"An acceptor received a Phase1a message in round ${phase1a.round} " +
          s"but is in round $round."
      )
      leader.send(LeaderInbound().withNack(Nack(round = round)))
      return
    }

    // Otherwise, we update our round and send back a Phase1b message to the
    // leader.
    round = phase1a.round
    val phase1b = Phase1b(
      groupIndex = groupIndex,
      acceptorIndex = index,
      round = round,
      info = states
        .iteratorFrom(round)
        .map({
          case (slot, state) =>
            Phase1bSlotInfo(slot = slot,
                            voteRound = state.voteRound,
                            voteValue = state.voteValue)
        })
        .toSeq
    )
    leader.send(LeaderInbound().withPhase1B(phase1b))
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    // If we receive an out of date round, we send back a nack to the leader.
    // Note that `src` is the address of the proxy leader, not the leader. We
    // don't want to send nacks to the proxy leader; we want to send them to
    // the actual leader.
    if (phase2a.round < round) {
      logger.debug(
        s"An acceptor received a Phase2a message in round ${phase2a.round} " +
          s"but is in round $round."
      )
      val leader = leaders(roundSystem.leader(phase2a.round))
      leader.send(LeaderInbound().withNack(Nack(round = round)))
      return
    }

    // Otherwise, update our round and send back a Phase2b message to the proxy
    // leader.
    round = phase2a.round
    states(phase2a.slot) = State(
      voteRound = round,
      voteValue = phase2a.commandBatchOrNoop
    )
    val proxyLeader = chan[ProxyLeader[Transport]](src, ProxyLeader.serializer)
    proxyLeader.send(
      ProxyLeaderInbound().withPhase2B(
        Phase2b(acceptorIndex = index, slot = phase2a.slot, round = round)
      )
    )
  }
}
