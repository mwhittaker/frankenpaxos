package frankenpaxos.matchmakermultipaxos

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
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
    .name("matchmakermultipaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakermultipaxos_acceptor_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val phase1NacksSentTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_acceptor_phase1_nacks_sent_total")
    .help("Total number of nacks sent in Phase 1.")
    .register()

  val phase2NacksSentTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_acceptor_phase2_nacks_sent_total")
    .help("Total number of nacks sent in Phase 2.")
    .register()

  val stalePersistedTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_acceptor_stale_persisted_total")
    .help("Total number of stale Persisted messages received.")
    .register()

  val persistedWatermark: Gauge = collectors.gauge
    .build()
    .name("matchmakermultipaxos_acceptor_persisted_watermark")
    .help("The persisted watermark.")
    .register()

  val phase2aPersistedTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_acceptor_phase2a_persisted_total")
    .help(
      "Total number of Phase2a messages that an acceptor received for a " +
        "persisted slot."
    )
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
  logger.check(config.acceptorAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = AcceptorInbound
  override val serializer = AcceptorInboundSerializer

  type Slot = Int

  @JSExportAll
  case class State(
      voteRound: Int,
      voteValue: CommandOrNoop
  )

  // Fields ////////////////////////////////////////////////////////////////////
  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  private val index = config.acceptorAddresses.indexOf(address)

  // For simplicity, we use a round robin round system for the leaders.
  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  @JSExport
  protected var round: Int = -1

  // This acceptor knows that all log entries less than persistedWatermark have
  // been persisted on at least f+1 replicas. The acceptor is free to garbage
  // collect all log entries less than persistedWatermark. If a leader contacts
  // the acceptor about one of these log entries, the acceptor will inform the
  // leader that the value was already chosen.
  @JSExport
  protected var persistedWatermark: Int = 0

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
        case Request.Phase1A(_)   => "Phase1a"
        case Request.Phase2A(_)   => "Phase2a"
        case Request.Persisted(_) => "Persisted"
        case Request.Empty =>
          logger.fatal("Empty AcceptorInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1A(r)   => handlePhase1a(src, r)
        case Request.Phase2A(r)   => handlePhase2a(src, r)
        case Request.Persisted(r) => handlePersisted(src, r)
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
      leader.send(LeaderInbound().withAcceptorNack(AcceptorNack(round = round)))
      metrics.phase1NacksSentTotal.inc()
      return
    }

    // Otherwise, we update our round and send back a Phase1b message to the
    // leader.
    round = phase1a.round
    val phase1b = Phase1b(
      round = round,
      acceptorIndex = index,
      persistedWatermark = persistedWatermark,
      info = states
        .iteratorFrom(phase1a.chosenWatermark)
        .flatMap({
          case (slot, state) =>
            // This reason for this `if` is very subtle. During an i/i+1
            // reconfiguration, a leader may send Phase2as in round i+1 while
            // performing Phase 1 in round i+1. If an acceptor receives a
            // Phase2a before the Phase1a, then it will return Phase1bs for
            // values in round i+1. We don't want this. So, if the vote round
            // of an entry is `round`, we don't send it back. The leader has
            // already determined a safe value to propose in the slot and
            // proposed it.
            if (state.voteRound < round) {
              Some(
                Phase1bSlotInfo(slot = slot,
                                voteRound = state.voteRound,
                                voteValue = state.voteValue)
              )
            } else {
              None
            }
        })
        .toSeq
    )
    leader.send(LeaderInbound().withPhase1B(phase1b))
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // If we receive a Phase2a for a slot that we know has been persisted, we
    // do not vote for it. Instead, we notify the leader that the value has
    // already been persisted.
    if (phase2a.slot < persistedWatermark) {
      leader.send(
        LeaderInbound().withPhase2B(
          Phase2b(slot = phase2a.slot,
                  round = phase2a.round,
                  acceptorIndex = index,
                  persisted = true)
        )
      )
      metrics.phase2aPersistedTotal.inc()
      return
    }

    // If we receive an out of date round, we send back a nack to the leader.
    if (phase2a.round < round) {
      logger.debug(
        s"An acceptor received a Phase2a message in round ${phase2a.round} " +
          s"but is in round $round."
      )
      leader.send(LeaderInbound().withAcceptorNack(AcceptorNack(round = round)))
      metrics.phase2NacksSentTotal.inc()
      return
    }

    // Otherwise, update our round and send back a Phase2b message to the
    // leader.
    round = phase2a.round
    states(phase2a.slot) = State(
      voteRound = round,
      voteValue = phase2a.value
    )
    leader.send(
      LeaderInbound().withPhase2B(
        Phase2b(slot = phase2a.slot,
                round = round,
                acceptorIndex = index,
                persisted = false)
      )
    )
  }

  private def handlePersisted(
      src: Transport#Address,
      persisted: Persisted
  ): Unit = {
    // Send back an ack. Note that we don't ignore stale persisted requests. If
    // we did this, then it's possible a leader with a stale persistedWatermark
    // would be completely ignored by the acceptors, which is not what we want.
    persistedWatermark =
      Math.max(persistedWatermark, persisted.persistedWatermark)
    val leader = chan[Leader[Transport]](src, Leader.serializer)
    leader.send(
      LeaderInbound().withPersistedAck(
        PersistedAck(
          acceptorIndex = index,
          persistedWatermark = persistedWatermark
        )
      )
    )

    // Garbage collect slots.
    states = states.dropWhile({
      case (slots, _) => round < persistedWatermark
    })
    metrics.persistedWatermark.set(persistedWatermark)
  }
}
