package frankenpaxos.scalog

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
import frankenpaxos.quorums.Grid
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.util
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
    // If a leader hasn't received Phase1bs for a while, it resends its
    // Phase1as. `resendPhase1asPeriod` determines how long the leader waits
    // before resending.
    resendPhase1asPeriod: java.time.Duration,
    // A leader flushes all of its channels to the proxy leaders after every
    // `flushPhase2asEveryN` Phase2a messages sent. For example, if
    // `flushPhase2asEveryN` is 1, then the leader flushes after every send.
    flushPhase2asEveryN: Int,
    // Every leader manages a log, implemented as a BufferMap. This is the
    // BufferMap's `logGrowSize`.
    logGrowSize: Int,
    electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    flushPhase2asEveryN = 1,
    logGrowSize = 5000,
    electionOptions = ElectionOptions.default,
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("scalog_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("scalog_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val leaderChangesTotal: Counter = collectors.counter
    .build()
    .name("scalog_leader_leader_changes_total")
    .help("Total number of leader changes.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("scalog_leader_resend_phase1as_total")
    .help("Total number of times the leader resent phase 1a messages.")
    .register()

  val stalePhase2bsTotal: Counter = collectors.counter
    .build()
    .name("scalog_leader_stale_phase2bs_total")
    .help("Total number of times a leader received a stale Phase2b.")
    .register()

  val phase2bAlreadyChosenTotal: Counter = collectors.counter
    .build()
    .name("scalog_leader_phase2b_already_chosen_total")
    .help(
      "Total number of times a leader received Phase2b for a slot that was " +
        "already chosen."
    )
    .register()
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.leaderAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override val serializer = LeaderInboundSerializer

  type AcceptorIndex = Int
  type Round = Int
  type Slot = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case object Inactive extends State

  @JSExportAll
  case class Phase1(
      phase1bs: mutable.Map[AcceptorIndex, Phase1b],
      pendingProposals: mutable.Buffer[ProposeCut],
      resendPhase1as: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase2(
      // The values that the leader is trying to get chosen.
      values: mutable.Map[Slot, GlobalCutOrNoop],
      // The Phase2b responses received from the acceptors.
      phase2bs: mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  private val index = config.leaderAddresses.indexOf(address)

  // Aggregator channel.
  private val aggregator: Chan[Aggregator[Transport]] =
    chan[Aggregator[Transport]](config.aggregatorAddress, Aggregator.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (a <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](a, Acceptor.serializer)

  // Other leader channels.
  private val otherLeaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses if a != address)
      yield chan[Leader[Transport]](a, Leader.serializer)

  private val roundSystem =
    new RoundSystem.ClassicRoundRobin(config.leaderAddresses.size)

  // If the leader is the active leader, then this is its round. If it is
  // inactive, then this is the largest active round it knows about.
  @JSExport
  protected var round: Round = roundSystem
    .nextClassicRound(leaderIndex = 0, round = -1)

  @JSExport
  protected val log: util.BufferMap[GlobalCutOrNoop] =
    new util.BufferMap(options.logGrowSize)

  // The next available slot in the log.
  @JSExport
  protected var nextSlot: Slot = 0

  // Every slot less than chosenWatermark has been chosen.
  @JSExport
  protected var chosenWatermark: Slot = 0

  // Leader election address. This field exists for the javascript
  // visualizations.
  @JSExport
  protected val electionAddress = config.leaderElectionAddresses(index)

  // Leader election participant.
  @JSExport
  protected val election = new Participant[Transport](
    address = electionAddress,
    transport = transport,
    logger = logger,
    addresses = config.leaderElectionAddresses,
    initialLeaderIndex = 0,
    options = options.electionOptions
  )
  election.register((leaderIndex) => {
    leaderChange(leaderIndex == index)
  })

  // The number of Phase2a messages since the last flush.
  private var numPhase2asSentSinceLastFlush: Int = 0

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    startPhase1(round, chosenWatermark)
  } else {
    Inactive
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase1asTimer(phase1a: Phase1a): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as",
      options.resendPhase1asPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        for (acceptor <- acceptors) {
          acceptor.send(AcceptorInbound().withPhase1A(phase1a))
        }
        t.start()
      }
    )
    t.start()
    t
  }

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

  // `maxPhase1bSlot(phase1b)` finds the largest slot present in `phase1b` or
  // -1 if no slots are present.
  private def maxPhase1bSlot(phase1b: Phase1b): Slot = {
    if (phase1b.info.isEmpty) {
      -1
    } else {
      phase1b.info.map(_.slot).max
    }
  }

  // Given a quorum of Phase1b messages, `safeValue` finds a value that is safe
  // to propose in a particular slot. If the Phase1b messages have at least one
  // vote in the slot, then the value with the highest vote round is safe.
  // Otherwise, everything is safe. In this case, we return Noop.
  private def safeValue(
      phase1bs: Iterable[Phase1b],
      slot: Slot
  ): GlobalCutOrNoop = {
    val slotInfos =
      phase1bs.flatMap(phase1b => phase1b.info.find(_.slot == slot))
    if (slotInfos.isEmpty) {
      GlobalCutOrNoop().withNoop(Noop())
    } else {
      slotInfos.maxBy(_.voteRound).voteValue
    }
  }

  private def processProposal(phase2: Phase2, proposal: ProposeCut): Unit = {
    // Send the Phase2as.
    val value = GlobalCutOrNoop().withGlobalCut(proposal.globalCut)
    val phase2a = Phase2a(
      slot = nextSlot,
      round = round,
      globalCutOrNoop = value
    )

    if (options.flushPhase2asEveryN == 1) {
      // If we flush every message, don't bother managing
      // `numPhase2asSentSinceLastFlush` or flushing channels.
      for (acceptor <- rand.shuffle(acceptors).take(config.f + 1)) {
        acceptor.send(AcceptorInbound().withPhase2A(phase2a))
      }
    } else {
      for (acceptor <- rand.shuffle(acceptors).take(config.f + 1)) {
        acceptor.sendNoFlush(AcceptorInbound().withPhase2A(phase2a))
      }
      numPhase2asSentSinceLastFlush += 1
    }

    if (numPhase2asSentSinceLastFlush >= options.flushPhase2asEveryN) {
      for (acceptor <- acceptors) {
        acceptor.flush()
      }
      numPhase2asSentSinceLastFlush = 0
    }

    // Update our metadata.
    logger.check(!phase2.values.contains(nextSlot))
    logger.check(!phase2.phase2bs.contains(nextSlot))
    phase2.values(nextSlot) = value
    phase2.phase2bs(nextSlot) = mutable.Map()
    nextSlot += 1
  }

  private def startPhase1(round: Round, chosenWatermark: Slot): Phase1 = {
    val phase1a = Phase1a(round = round, chosenWatermark = chosenWatermark)
    for (acceptor <- acceptors) {
      acceptor.send(AcceptorInbound().withPhase1A(phase1a))
    }

    Phase1(
      phase1bs = mutable.Map(),
      pendingProposals = mutable.Buffer(),
      resendPhase1as = makeResendPhase1asTimer(phase1a)
    )
  }

  private def leaderChange(isNewLeader: Boolean): Unit = {
    metrics.leaderChangesTotal.inc()

    (state, isNewLeader) match {
      case (Inactive, false) =>
        // Do nothing.
        ()
      case (phase1: Phase1, false) =>
        phase1.resendPhase1as.stop()
        state = Inactive
      case (phase2: Phase2, false) =>
        state = Inactive
      case (Inactive, true) =>
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
      case (phase1: Phase1, true) =>
        phase1.resendPhase1as.stop()
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
      case (phase2: Phase2, true) =>
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import LeaderInbound.Request

    val label =
      inbound.request match {
        case Request.Phase1B(_)           => "Phase1b"
        case Request.ProposeCut(_)        => "ProposeCut"
        case Request.Phase2B(_)           => "Phase2b"
        case Request.RawCutChosen(_)      => "RawCutChosen"
        case Request.LeaderInfoRequest(_) => "LeaderInfoRequest"
        case Request.Recover(_)           => "Recover"
        case Request.Nack(_)              => "Nack"
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1B(r)           => handlePhase1b(src, r)
        case Request.ProposeCut(r)        => handleProposeCut(src, r)
        case Request.Phase2B(r)           => handlePhase2b(src, r)
        case Request.RawCutChosen(r)      => handleRawCutChosen(src, r)
        case Request.LeaderInfoRequest(r) => handleLeaderInfoRequest(src, r)
        case Request.Recover(r)           => handleRecover(src, r)
        case Request.Nack(r)              => handleNack(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handlePhase1b(src: Transport#Address, phase1b: Phase1b): Unit = {
    state match {
      case Inactive | _: Phase2 =>
        logger.debug(
          s"A leader received a Phase1b message but is not in Phase1. Its " +
            s"state is $state. The Phase1b message is being ignored."
        )

      case phase1: Phase1 =>
        // Ignore messages from stale rounds.
        if (phase1b.round != round) {
          logger.debug(
            s"A leader received a Phase1b message in round ${phase1b.round} " +
              s"but is in round $round. The Phase1b is being ignored."
          )
          // If phase1b.round were larger than round, then we would have
          // received a nack instead of a Phase1b.
          logger.checkLt(phase1b.round, round)
          return
        }

        // Wait until we have a quorum of responses.
        phase1.phase1bs(phase1b.acceptorIndex) = phase1b
        if (phase1.phase1bs.size < config.f + 1) {
          return
        }

        // Find the largest slot with a vote.
        val maxSlot = phase1.phase1bs.values.map(maxPhase1bSlot).max

        // Now, we iterate from chosenWatermark to maxSlot proposing safe
        // values to fill in the log.
        val values = mutable.Map[Slot, GlobalCutOrNoop]()
        val phase2bs = mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]()
        for (slot <- chosenWatermark to maxSlot) {
          val value = safeValue(phase1.phase1bs.values, slot)
          values(slot) = value
          phase2bs(slot) = mutable.Map()

          for (acceptor <- acceptors) {
            acceptor.send(
              AcceptorInbound().withPhase2A(
                Phase2a(slot = slot, round = round, globalCutOrNoop = value)
              )
            )
          }
        }

        // We've filled in every slot until and including maxSlot, so the next
        // slot is maxSlot + 1.
        nextSlot = maxSlot + 1

        // Update our state.
        phase1.resendPhase1as.stop()
        val phase2 = Phase2(values = values, phase2bs = phase2bs)
        state = phase2

        // Process any pending client requests.
        for (pendingProposal <- phase1.pendingProposals) {
          processProposal(phase2, pendingProposal)
        }
    }
  }

  private def handleProposeCut(
      src: Transport#Address,
      proposeCut: ProposeCut
  ): Unit = {
    state match {
      case Inactive =>
        logger.debug(
          "An inactive leader received a handleProposeCut message. The " +
            "message is being ignored."
        )

      case phase1: Phase1 =>
        // We'll process the proposal after we've finished phase 1.
        phase1.pendingProposals += proposeCut

      case phase2: Phase2 =>
        processProposal(phase2, proposeCut)
    }
  }

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = {
    // Ignore messages from stale rounds.
    if (phase2b.round != round) {
      logger.debug(
        s"A leader received a Phase2b message in round ${phase2b.round} " +
          s"but is in round $round. The Phase2b is being ignored."
      )
      metrics.stalePhase2bsTotal.inc()
      return
    }

    // Ignore messages for slots that have already been chosen.
    if (phase2b.slot < chosenWatermark || log.get(phase2b.slot).isDefined) {
      logger.debug(
        s"A leader received a Phase2b message in slot ${phase2b.slot} " +
          s"but that slot has already been chosen. The Phase2b message " +
          s"is being ignored."
      )
      metrics.phase2bAlreadyChosenTotal.inc()
      return
    }

    state match {
      case Inactive | _: Phase1 =>
        logger.debug(
          s"Leader received a Phase2b message but is not in Phase 2. It's " +
            s"state is $state. The Phase2b message is being ignored."
        )

      case phase2: Phase2 =>
        // Wait until we have a write quorum.
        val phase2bs = phase2.phase2bs(phase2b.slot)
        phase2bs(phase2b.acceptorIndex) = phase2b
        if (phase2bs.size < config.f + 1) {
          return
        }

        // Inform the aggregator and other leaders that the value has
        // been chosen.
        val value = phase2.values(phase2b.slot)
        val chosen = RawCutChosen(slot = phase2b.slot, rawCutOrNoop = value)
        aggregator.send(AggregatorInbound().withRawCutChosen(chosen))
        for (leader <- otherLeaders) {
          leader.send(LeaderInbound().withRawCutChosen(chosen))
        }

        // Update our metadata.
        phase2.values.remove(phase2b.slot)
        phase2.phase2bs.remove(phase2b.slot)
        log.put(phase2b.slot, value)
        while (log.get(chosenWatermark).isDefined) {
          chosenWatermark += 1
        }
    }
  }

  private def handleRawCutChosen(
      src: Transport#Address,
      rawCutChosen: RawCutChosen
  ): Unit = {
    log.put(rawCutChosen.slot, rawCutChosen.rawCutOrNoop)
    while (log.get(chosenWatermark).isDefined) {
      chosenWatermark += 1
    }
  }

  private def handleLeaderInfoRequest(
      src: Transport#Address,
      leaderInfoRequest: LeaderInfoRequest
  ): Unit = {
    state match {
      case Inactive =>
        // We're inactive, so we ignore the request. The active leader will
        // respond to the request.
        ()

      case _: Phase1 | _: Phase2 =>
        aggregator.send(
          AggregatorInbound()
            .withLeaderInfoReply(LeaderInfoReply(round = round))
        )
    }
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    log.get(recover.slot) match {
      case Some(rawCutOrNoop) =>
        aggregator.send(
          AggregatorInbound().withRawCutChosen(
            RawCutChosen(slot = recover.slot, rawCutOrNoop = rawCutOrNoop)
          )
        )
        return

      case None =>
        // Do nothing.
        ()
    }

    state match {
      case Inactive | _: Phase1 =>
        logger.debug(
          s"A leader received a Recover but is not in Phase2. It's state is " +
            s"$state. The Recover is being ignored"
        )

      case phase2: Phase2 =>
        phase2.values.get(recover.slot) match {
          case Some(globalCutOrNoop) =>
            // The values is pending. We resend Phase2as to make sure it gets
            // chosen.
            for (acceptor <- acceptors) {
              acceptor.send(
                AcceptorInbound().withPhase2A(
                  Phase2a(slot = recover.slot,
                          round = round,
                          globalCutOrNoop = globalCutOrNoop)
                )
              )
            }

          case None =>
            // We don't have the cut pending, and it's not in our log. This is
            // a rare situation, so we just ignore it for now.
            ()
        }
    }
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    if (nack.round <= round) {
      logger.debug(
        s"A Leader received a Nack message with round ${nack.round} but is " +
          s"already in round $round. The Nack is being ignored."
      )
      return
    }

    state match {
      case Inactive =>
        // Do nothing.
        round = nack.round
      case _: Phase1 | _: Phase2 =>
        round =
          roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
        leaderChange(isNewLeader = true)
    }
  }
}
