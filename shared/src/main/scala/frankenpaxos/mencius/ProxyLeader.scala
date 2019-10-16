package frankenpaxos.mencius

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
object ProxyLeaderInboundSerializer
    extends ProtoSerializer[ProxyLeaderInbound] {
  type A = ProxyLeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object ProxyLeader {
  val serializer = ProxyLeaderInboundSerializer
}

@JSExportAll
case class ProxyLeaderOptions(
    // A proxy leader flushes all of its channels to the acceptors after every
    // `flushPhase2asEveryN` Phase2a messages sent. For example, if
    // `flushPhase2asEveryN` is 1, then the proxy leader flushes after every
    // send.
    flushPhase2asEveryN: Int,
    measureLatencies: Boolean
)

@JSExportAll
object ProxyLeaderOptions {
  val default = ProxyLeaderOptions(
    flushPhase2asEveryN = 1,
    measureLatencies = true
  )
}

@JSExportAll
class ProxyLeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("mencius_proxy_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("mencius_proxy_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class ProxyLeader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ProxyLeaderOptions = ProxyLeaderOptions.default,
    metrics: ProxyLeaderMetrics = new ProxyLeaderMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  import ProxyLeader._
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ProxyLeaderInbound
  override val serializer = ProxyLeaderInboundSerializer

  type AcceptorIndex = Int
  type Slot = Int

  @JSExportAll
  case class SlotRound(
      slotStartInclusive: Slot,
      slotEndExclusive: Slot,
      round: Int
  )

  @JSExportAll
  sealed trait State

  @JSExportAll
  case class PendingPhase2a(
      phase2a: Phase2a,
      phase2bs: mutable.Map[AcceptorIndex, Phase2b]
  ) extends State

  @JSExportAll
  case class PendingPhase2aNoopRange(
      phase2aNoopRange: Phase2aNoopRange,
      // phase2bNoopRanges has one entry for every acceptor group. Each entry
      // records the Phase2bNoopRange messages returned by the group.
      phase2bNoopRanges: mutable.Buffer[
        mutable.Map[AcceptorIndex, Phase2bNoopRange]
      ]
  ) extends State

  @JSExportAll
  object Done extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // Leader channels.
  private val leaders: Seq[Seq[Chan[Leader[Transport]]]] =
    for (group <- config.leaderAddresses) yield {
      for (address <- group)
        yield chan[Leader[Transport]](address, Leader.serializer)
    }

  // Acceptor channels.
  private val acceptors: Seq[Seq[Seq[Chan[Acceptor[Transport]]]]] =
    for (groups <- config.acceptorAddresses) yield {
      for (group <- groups) yield {
        for (address <- group)
          yield chan[Acceptor[Transport]](address, Acceptor.serializer)
      }
    }

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (address <- config.replicaAddresses)
      yield chan[Replica[Transport]](address, Replica.serializer)

  // A round system used to figure out which leader groups are in charge of
  // which slots. For example, if we have 5 leader groups and we're leader
  // group 1 and we'd like to know which slot to use after slot 20, we can call
  // slotSystem.nextClassicRound(1, 20).
  private val slotSystem =
    new RoundSystem.ClassicRoundRobin(config.numLeaderGroups)

  // The number of Phase2a messages since the last flush.
  private var numPhase2asSentSinceLastFlush: Int = 0

  @JSExport
  protected var states = mutable.Map[SlotRound, State]()

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

  // See Leader for documentation.
  private def acceptorGroupIndexBySlot(
      leaderGroupIndex: Int,
      slot: Slot
  ): Int = {
    (slot / config.numLeaderGroups) % config
      .acceptorAddresses(leaderGroupIndex)
      .size
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ProxyLeaderInbound.Request

    val label =
      inbound.request match {
        case Request.HighWatermark(_)    => "HighWatermark"
        case Request.Phase2A(_)          => "Phase2a"
        case Request.Phase2ANoopRange(_) => "Phase2aNoopRange"
        case Request.Phase2B(_)          => "Phase2b"
        case Request.Phase2BNoopRange(_) => "Phase2bNoopRange"
        case Request.Empty =>
          logger.fatal("Empty ProxyLeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.HighWatermark(r)    => handleHighWatermark(src, r)
        case Request.Phase2A(r)          => handlePhase2a(src, r)
        case Request.Phase2ANoopRange(r) => handlePhase2aNoopRange(src, r)
        case Request.Phase2B(r)          => handlePhase2b(src, r)
        case Request.Phase2BNoopRange(r) => handlePhase2bNoopRange(src, r)
        case Request.Empty =>
          logger.fatal("Empty ProxyLeaderInbound encountered.")
      }
    }
  }

  private def handleHighWatermark(
      src: Transport#Address,
      highWatermark: HighWatermark
  ): Unit = {
    for (group <- leaders; leader <- group) {
      leader.send(LeaderInbound().withHighWatermark(highWatermark))
    }
  }

  private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
    val slotround = SlotRound(slotStartInclusive = phase2a.slot,
                              slotEndExclusive = phase2a.slot + 1,
                              round = phase2a.round)
    states.get(slotround) match {
      case Some(_) =>
        logger.debug(
          s"A ProxyLeader received a Phase2a in slot ${phase2a.slot} and " +
            s"round ${phase2a.round}, but it already received this Phase2a. " +
            s"The ProxyLeader is ignoring the message."
        )

      case None =>
        // Select the appropriate acceptor group, and then randomly select a
        // thrifty quorum of it. Relay the Phase2a message to the quorum
        val leaderGroupIndex = slotSystem.leader(phase2a.slot)
        val group = acceptors(leaderGroupIndex)(
          acceptorGroupIndexBySlot(leaderGroupIndex, phase2a.slot)
        )
        val quorum = scala.util.Random.shuffle(group).take(config.quorumSize)
        if (options.flushPhase2asEveryN == 1) {
          quorum.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))
        } else {
          quorum.foreach(_.sendNoFlush(AcceptorInbound().withPhase2A(phase2a)))
          numPhase2asSentSinceLastFlush += 1
          if (numPhase2asSentSinceLastFlush >= options.flushPhase2asEveryN) {
            for (groups <- acceptors; group <- groups; acceptor <- group) {
              acceptor.flush()
            }
            numPhase2asSentSinceLastFlush = 0
          }
        }

        // Update our state.
        states(slotround) =
          PendingPhase2a(phase2a = phase2a, phase2bs = mutable.Map())
    }
  }

  private def handlePhase2aNoopRange(
      src: Transport#Address,
      phase2a: Phase2aNoopRange
  ): Unit = {
    val slotround = SlotRound(slotStartInclusive = phase2a.slotStartInclusive,
                              slotEndExclusive = phase2a.slotEndExclusive,
                              round = phase2a.round)
    states.get(slotround) match {
      case Some(_) =>
        logger.debug(
          s"A ProxyLeader received a Phase2aNoopRange in slots " +
            s"${phase2a.slotStartInclusive}-${phase2a.slotEndExclusive} and " +
            s"round ${phase2a.round}, but it already received this " +
            s"Phase2aNoopRange. The ProxyLeader is ignoring the message."
        )

      case None =>
        // Select the appropriate acceptor groups, and then randomly select
        // thrifty quorums. Relay the Phase2a message to every quorum.
        val leaderGroupIndex = slotSystem.leader(phase2a.slotStartInclusive)
        for (group <- acceptors(leaderGroupIndex)) {
          val quorum = scala.util.Random.shuffle(group).take(config.quorumSize)
          if (options.flushPhase2asEveryN == 1) {
            quorum.foreach(
              _.send(AcceptorInbound().withPhase2ANoopRange(phase2a))
            )
          } else {
            quorum.foreach(
              _.sendNoFlush(AcceptorInbound().withPhase2ANoopRange(phase2a))
            )
            numPhase2asSentSinceLastFlush += 1
            if (numPhase2asSentSinceLastFlush >= options.flushPhase2asEveryN) {
              for (groups <- acceptors; group <- groups; acceptor <- group) {
                acceptor.flush()
              }
              numPhase2asSentSinceLastFlush = 0
            }
          }
        }

        // Update our state.
        states(slotround) = PendingPhase2aNoopRange(
          phase2aNoopRange = phase2a,
          phase2bNoopRanges = mutable.Buffer.fill(
            config.acceptorAddresses(leaderGroupIndex).size
          )(mutable.Map())
        )
    }
  }

  private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    val slotround = SlotRound(slotStartInclusive = phase2b.slot,
                              slotEndExclusive = phase2b.slot + 1,
                              round = phase2b.round)
    states.get(slotround) match {
      case None =>
        logger.fatal(
          s"A ProxyLeader received a Phase2b in slot ${phase2b.slot} and " +
            s"round ${phase2b.round}, but it never sent a Phase2a in this " +
            s"slot and round."
        )

      case Some(Done) =>
        logger.debug(
          s"A ProxyLeader received a Phase2b in slot ${phase2b.slot} and " +
            s"round ${phase2b.round}, but it has already chosen a value in " +
            s"this slot and round. The Phase2b message is ignored."
        )

      case Some(_: PendingPhase2aNoopRange) =>
        logger.debug(
          s"A ProxyLeader received a Phase2b in slot ${phase2b.slot} and " +
            s"round ${phase2b.round}, but it is processing a " +
            s"Phase2aNoopRange in this slot. The Phase2b message is being " +
            s"ignored."
        )

      case Some(pending: PendingPhase2a) =>
        // Wait until we receive a quorum of Phase2bs.
        val phase2bs = pending.phase2bs
        phase2bs(phase2b.acceptorIndex) = phase2b
        if (phase2bs.size < config.quorumSize) {
          return
        }

        // Let the replicas know that the value has been chosen.
        replicas.foreach(
          _.send(
            ReplicaInbound().withChosen(
              Chosen(slot = phase2b.slot,
                     commandBatchOrNoop = pending.phase2a.commandBatchOrNoop)
            )
          )
        )

        // Update our state.
        states(slotround) = Done
    }
  }

  private def handlePhase2bNoopRange(
      src: Transport#Address,
      phase2b: Phase2bNoopRange
  ): Unit = {
    val slotround = SlotRound(slotStartInclusive = phase2b.slotStartInclusive,
                              slotEndExclusive = phase2b.slotEndExclusive,
                              round = phase2b.round)
    states.get(slotround) match {
      case None =>
        logger.fatal(
          s"A ProxyLeader received a Phase2bNoopRange in slots " +
            s"${phase2b.slotStartInclusive}-${phase2b.slotEndExclusive} and " +
            s"round ${phase2b.round}, but it never sent a Phase2aNoopRange " +
            s"in these slots and round."
        )

      case Some(Done) =>
        logger.debug(
          s"A ProxyLeader received a Phase2bNoopRange in slots " +
            s"${phase2b.slotStartInclusive}-${phase2b.slotEndExclusive} and " +
            s"round ${phase2b.round}, but it has already chosen a value in " +
            s"these slots and round. The Phase2bNoopRange message is ignored."
        )

      case Some(_: PendingPhase2a) =>
        logger.debug(
          s"A ProxyLeader received a Phase2bNoopRange in slots " +
            s"${phase2b.slotStartInclusive}-${phase2b.slotEndExclusive} and " +
            s"round ${phase2b.round}, but it is processing a Phase2a in this " +
            s"slot. The Phase2bNoopRange message is being ignored."
        )

      case Some(pending: PendingPhase2aNoopRange) =>
        // Wait until we receive a quorum of Phase2bs from every acceptor group.
        val phase2bs = pending.phase2bNoopRanges
        phase2bs(phase2b.acceptorGroupIndex)(phase2b.acceptorIndex) = phase2b
        if (phase2bs.exists(_.size < config.quorumSize)) {
          return
        }

        // Let the replicas know that the value has been chosen.
        replicas.foreach(
          _.send(
            ReplicaInbound()
              .withChosenNoopRange(
                ChosenNoopRange(
                  slotStartInclusive =
                    pending.phase2aNoopRange.slotStartInclusive,
                  slotEndExclusive = pending.phase2aNoopRange.slotEndExclusive
                )
              )
          )
        )

        // Update our state.
        states(slotround) = Done
    }
  }
}
