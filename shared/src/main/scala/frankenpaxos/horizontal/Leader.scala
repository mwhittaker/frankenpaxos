package frankenpaxos.horizontal

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
    // A Leader implements its log as a BufferMap. `logGrowSize` is the
    // `growSize` argument used to construct the BufferMap.
    logGrowSize: Int,
    // Leaders use timeouts to re-send requests and ensure liveness. These
    // durations determine how long a leader waits before re-sending a request.
    resendMatchRequestsPeriod: java.time.Duration,
    resendReconfigurePeriod: java.time.Duration,
    resendPhase1asPeriod: java.time.Duration,
    resendPhase2asPeriod: java.time.Duration,
    resendExecutedWatermarkRequestsPeriod: java.time.Duration,
    resendPersistedPeriod: java.time.Duration,
    resendGarbageCollectsPeriod: java.time.Duration,
    // The active leader sends a chosen watermark to all the other leaders
    // after every `sendChosenWatermarkEveryN` chosen commands. This ensures
    // that during a leader change, all the leaders have a relatively up to
    // date chosen watermark.
    sendChosenWatermarkEveryN: Int,
    // Leaders use a ClassicStutteredRound round system. This is the stutter.
    stutter: Int,
    electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    logGrowSize = 1000,
    resendMatchRequestsPeriod = java.time.Duration.ofSeconds(5),
    resendReconfigurePeriod = java.time.Duration.ofSeconds(5),
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    resendPhase2asPeriod = java.time.Duration.ofSeconds(5),
    resendExecutedWatermarkRequestsPeriod = java.time.Duration.ofSeconds(5),
    resendPersistedPeriod = java.time.Duration.ofSeconds(5),
    resendGarbageCollectsPeriod = java.time.Duration.ofSeconds(5),
    sendChosenWatermarkEveryN = 100,
    stutter = 1000,
    electionOptions = ElectionOptions.default,
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("horizontal_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val activeLeaderReceivedChosenTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_active_leader_received_chosen_total")
    .help("Total number of times an active leader received a Chosen.")
    .register()

  //

  val reconfigurationLatency: Summary = collectors.summary
    .build()
    .name("horizontal_leader_reconfiguration_latency")
    .labelNames("phase")
    .help("Latency (in milliseconds) of a reconfiguration phase.")
    .register()

  val staleMatchRepliesTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_match_replies_total")
    .help("Total number of stale MatchReplies received.")
    .register()

  val stalePhase1bsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_phase1bs_total")
    .help("Total number of stale Phase1bs received.")
    .register()

  val stalePhase2bsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_phase2bs_total")
    .help("Total number of stale Phase2bs received.")
    .register()

  val phase2bAlreadyChosenTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_phase2b_already_chosen_total")
    .help(
      "Total number of Phase2bs received for a slot that was already chosen."
    )
    .register()

  val staleMatchmakerNackTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_matchmaker_nack_total")
    .help("Total number of stale MatchmakerNacks received.")
    .register()

  val staleAcceptorNackTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_acceptor_nack_total")
    .help("Total number of stale AcceptorNacks received.")
    .register()

  val stopBeingLeaderTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stop_being_leader_total")
    .help("Total number of times a node stops being a leader.")
    .register()

  val becomeLeaderTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_become_leader_total")
    .help("Total number of times a node becomes the leader.")
    .register()

  val becomeIIPlusOneLeaderTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_become_i_i_plus_one_leader_total")
    // "yes" if the i/i+1 path is taken and "no" if the normal reconfiguration
    // path is taken.
    .labelNames("successful")
    .help("Total number of times a node attemps an i/i+1 leader change.")
    .register()

  val gcDoneDuringLeaderChange: Counter = collectors.counter
    .build()
    .name("horizontal_leader_gc_done_during_leader_change")
    .help("Total number of times an i/i+1 leader change happens with GC done.")
    .register()

  // After the matchmaking phase, we have to perform Phase 1 with some set of n
  // previous rounds. previousNumberOfRoundsTotal records the frequency of n.
  // We make this a Counter instead of a Summary because it's likely that there
  // won't be many different values of n.
  val previousNumberOfRoundsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_previous_number_of_rounds_total")
    // "yes" if the i/i+1 path is taken and "no" if the normal reconfiguration
    // path is taken.
    .labelNames("n")
    .help(
      "The number of previous rounds with which we have to perform Phase 1."
    )
    .register()

  val phase1NumEntries: Summary = collectors.summary
    .build()
    .name("horizontal_leader_phase1_num_entries")
    .help(
      "The number of log entries we receive in Phase 1 in which we have " +
        "to get some value chosen."
    )
    .register()

  val resendMatchRequestsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_resend_match_requests_total")
    .help("Total number of times the leader resent MatchRequest messages.")
    .register()

  val resendReconfigureTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_resend_reconfigure_total")
    .help("Total number of times the leader resent Reconfigure messages.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_resend_phase1as_total")
    .help("Total number of times the leader resent Phase1a messages.")
    .register()

  val resendPhase2asTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_resend_phase2as_total")
    .help("Total number of times the leader resent Phase2a messages.")
    .register()

  val resendExecutedWatermarkRequestsTotal: Counter = collectors.counter
    .build()
    .name(
      "horizontal_leader_resend_executed_watermark_requests_total"
    )
    .help(
      "Total number of times the leader resent ExecutedWatermarkRequest messages."
    )
    .register()

  val resendPersistedTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_resend_persisted_total")
    .help("Total number of times the leader resent Persisted messages.")
    .register()

  val resendGarbageCollectsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_resend_garbage_collect_total")
    .help("Total number of times the leader resent GarbageCollect messages.")
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
  type ReplicaIndex = Int
  type Round = Int
  type Slot = Int

  @JSExportAll
  sealed trait Phase

  @JSExportAll
  case class Phase1(
      phase1bs: mutable.Buffer[mutable.Map[AcceptorIndex, Phase1b]],
      pendingClientRequests: mutable.Buffer[ClientRequest],
      resendPhase1as: Transport#Timer
  ) extends Phase

  @JSExportAll
  case class Phase2(
      // The next free slot in the log, or None if the chunk is out of slots.
      var nextSlot: Option[Slot],
      // The values that the leader is trying to get chosen.
      values: mutable.Map[Slot, Value],
      // The Phase2b responses received from the acceptors.
      phase2bs: mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]
  ) extends Phase

  @JSExportAll
  case class Chunk(
      // The first and last slot owned by this chunk. The names are borrowed
      // from [1]. lastSlot is an Optional because a chunk does not always have
      // a last slot.
      //
      // [1]: scholar.google.com/scholar?cluster=5995547042370880548
      firstSlot: Slot,
      lastSlot: Option[Slot],
      // The configuration (a.k.a. quorum system) used in this chunk.
      quorumSystem: QuorumSystem[AcceptorIndex],
      // The state of the chunk.
      phase: Phase
  )

  @JSExportAll
  sealed trait State

  // One of the proposers is elected leader. All other proposers are inactive.
  @JSExportAll
  case class Inactive(
      // When this inactive proposer becomes a leader, it begins in the first
      // round it owns larger than `round`.
      round: Round
  ) extends State

  @JSExportAll
  case class Active(
      round: Round,
      // We divide the log into chunks, where each chunk is owned by a single
      // configuration. This similar to the approach taken in [1]. chunks
      // contains a list of all active chunks, sorted by firstSlot. We say a
      // chunk is _defunct_ if the leader knows that all of its slots are
      // chosen. A chunk that is not defunct is _active_.
      //
      // We may want to implement `chunks` as some sort of binary search tree
      // so that we can efficiently find the chunk that owns a slot, but in the
      // normal case, we have very few active chunks.
      //
      // [1]: scholar.google.com/scholar?cluster=5995547042370880548
      chunks: mutable.Buffer[Chunk]
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  private val index = config.leaderAddresses.indexOf(address)

  // Channels to all the _other_ leaders.
  private val otherLeaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses if a != address)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (a <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](a, Acceptor.serializer)

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // The log of commands. We implement the log as a BufferMap as opposed to
  // something like a SortedMap for efficiency.
  @JSExport
  protected val log = new util.BufferMap[Value](options.logGrowSize)

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
    if (leaderIndex == index) {
      logger.info("Becoming leader due to leader election!")
      becomeLeader(getNextRound(state))
    } else {
      stopBeingLeader()
    }
  })

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    // In the first round, we use a predetermined set of acceptors. This is not
    // necessary, but it is convenient for some benchmarks.
    val quorumSystem =
      new SimpleMajority((0 until (2 * config.f + 1)).toSet, seed)
    startMatchmaking(round = 0,
                     pendingClientRequests = mutable.Buffer(),
                     quorumSystem,
                     QuorumSystem.toProto(quorumSystem))
  } else {
    Inactive(round = -1)
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase1asTimer(
      phase1a: Phase1a,
      acceptorIndices: Set[AcceptorIndex]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as",
      options.resendPhase1asPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        for (index <- acceptorIndices) {
          acceptors(index).send(AcceptorInbound().withPhase1A(phase1a))
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPhase2asTimer(): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase2as",
      options.resendPhase2asPeriod,
      () => {
        metrics.resendPhase2asTotal.inc()

        val phase2 = state match {
          case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
              _: Phase1 =>
            logger.fatal(
              s"The resendPhase2as timer was triggered but the leader is " +
                s"not in Phase2. Its state is $state."
            )
          case phase2: Phase2                       => phase2
          case phase2Matchmaking: Phase2Matchmaking => phase2Matchmaking.phase2
          // This timer might have been from the old Phase 2, but we run the
          // new Phase 2 anyway since that's the only one needed for liveness.
          case phase212: Phase212 => phase212.newPhase2
          case phase22: Phase22   => phase22.newPhase2
        }

        // TODO(mwhittaker): Pass this value in as a parameter.
        for (slot <- chosenWatermark until chosenWatermark + 10) {
          phase2.values.get(slot) match {
            case Some(value) =>
              val phase2a =
                Phase2a(slot = slot, round = getRound(state), value = value)
              for (index <- phase2.quorumSystem.nodes) {
                acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
              }

            case None =>
              logger.debug(
                s"The resendPhase2as timer was triggered but there is no " +
                  s"pending value for slot $slot."
              )
          }
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

  private def getRound(state: State): Round = {
    state match {
      case inactive: Inactive => inactive.round
      case active: Active     => active.round
    }
  }

  // Returns the Chunk that owns `slot`. If no such Chunk exists (i.e. if
  // `slot` is less than `chunks(0)`
  private def getChunk(
      chunks: mutable.Buffer[Chunk],
      slot: Slot
  ): Option[Chunk] = {
    logger.check(!chunks.isEmpty)

    for (chunk <- chunks.reverseIterator) {
      if (slot >= chunk.firstSlot) {
        return Some(chunk)
      }
    }

    None
  }

  // Returns the smallest round owned by this leader that is larger than any
  // round in `state`.
  private def getNextRound(state: State): Round =
    roundSystem.nextClassicRound(leaderIndex = index, round = getRound(state))

  private def stopPhaseTimers(phase: Phase): Unit = {
    phase match {
      case phase1: Phase1 => phase1.resendPhase1as.stop()
      case phase2: Phase2 =>
    }
  }

  private def stopTimers(state: State): Unit = {
    state match {
      case _: Inactive => ()

      case active: Active =>
        for (chunk <- active.chunks) {
          stopPhaseTimers(chunk.phase)
        }
    }
  }

  private def choose(
      slot: Slot,
      value: Value
  ): mutable.Buffer[(Slot, Configuration)] = {
    log.put(slot, value)

    val configurations = mutable.Buffer[(Slot, Configuration)]()
    while (true) {
      log.get(chosenWatermark) match {
        case Some(value) =>
          val slot = chosenWatermark
          chosenWatermark += 1
          value.value match {
            case Value.Value.Command(_) | Value.Value.Noop(_) =>
              // Do nothing.
              {}

            case Value.Value.Configuration(configuration) =>
              configurations.append((slot, configuration))

            case Value.Value.Empty =>
              logger.fatal("Empty Value encountered.")
          }

        case None =>
          // We "execute" the log in prefix order, so if we hit an empty slot,
          // we have to stop "executing".
          return configurations
      }
    }

    configurations
  }

  // TODO(mwhittaker): Keep?
  //
  // private def pendingClientRequests(
  //     state: State
  // ): mutable.Buffer[ClientRequest] = {
  //   state match {
  //     case _: Inactive | _: Phase2 | _: Phase2Matchmaking | _: Phase212 |
  //         _: Phase22 =>
  //       mutable.Buffer()
  //     case matchmaking: Matchmaking =>
  //       matchmaking.pendingClientRequests
  //     case waitingForReconfigure: WaitingForNewMatchmakers =>
  //       waitingForReconfigure.pendingClientRequests
  //     case phase1: Phase1 =>
  //       phase1.pendingClientRequests
  //   }
  // }

  // Given a set of Phase1b messages, `safeValue` finds a value that is safe to
  // propose in a particular slot. If the Phase1b messages have at least one
  // vote in the slot, then the value with the highest vote round is safe.
  // Otherwise, everything is safe. In this case, we return Noop.
  private def safeValue(
      phase1bs: Iterable[Phase1b],
      slot: Slot
  ): CommandOrNoop = {
    val slotInfos =
      phase1bs.flatMap(phase1b => phase1b.info.find(_.slot == slot))
    if (slotInfos.isEmpty) {
      CommandOrNoop().withNoop(Noop())
    } else {
      slotInfos.maxBy(_.voteRound).voteValue
    }
  }

  private def stopBeingLeader(): Unit = {
    metrics.stopBeingLeaderTotal.inc()
    stopTimers(state)
    state = Inactive(getRound(state))
  }

  private def becomeLeader(newRound: Round): Unit = {
    logger.debug(s"Becoming leader in round $newRound.")

    logger.checkGt(newRound, getRound(state))
    logger.check(roundSystem.leader(newRound) == index)
    metrics.becomeLeaderTotal.inc()

    stopTimers(state)
    val (qs, qsp) = getRandomQuorumSystem(config.numAcceptors)
    state = startMatchmaking(newRound, pendingClientRequests(state), qs, qsp)
  }

  private def becomeIIPlusOneLeader(
      qs: QuorumSystem[AcceptorIndex],
      qsp: QuorumSystemProto
  ): Unit = {
    state match {
      case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
          _: Phase1 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        metrics.becomeIIPlusOneLeaderTotal.labels("no").inc()
        becomeLeader(getNextRound(state))

      case phase2: Phase2 =>
        if (roundSystem.leader(phase2.round + 1) == index) {
          // Update metrics.
          metrics.becomeIIPlusOneLeaderTotal.labels("yes").inc()
          if (phase2.gc == Done) {
            metrics.gcDoneDuringLeaderChange.inc()
          }

          val matchmaking = startMatchmaking(
            round = phase2.round + 1,
            pendingClientRequests = mutable.Buffer(),
            qs,
            qsp
          )
          stopTimers(state)
          state = Phase2Matchmaking(phase2, matchmaking, System.nanoTime)
        } else {
          metrics.becomeIIPlusOneLeaderTotal.labels("no").inc()
          becomeLeader(getNextRound(state))
        }
    }
  }

  def processPhase1b(
      phase1: Phase1,
      phase1b: Phase1b
  ): Option[mutable.SortedMap[Slot, CommandOrNoop]] = {
    // Ignore messages from stale rounds.
    if (phase1b.round != phase1.round) {
      logger.debug(
        s"A leader received a Phase1b message in round ${phase1b.round} " +
          s"but is in round ${phase1.round}. The Phase1b is being ignored."
      )
      metrics.stalePhase1bsTotal.inc()
      // We can't receive phase 1bs from the future.
      logger.checkLt(phase1b.round, phase1.round)
      return None
    }

    // Wait until we have a read quorum for every pending round.
    logger.checkGt(phase1.pendingRounds.size, 0)
    phase1.phase1bs(phase1b.acceptorIndex) = phase1b
    for (round <- phase1.acceptorToRounds(phase1b.acceptorIndex)) {
      if (phase1
            .previousQuorumSystems(round)
            .isSuperSetOfReadQuorum(phase1.phase1bs.keys.toSet)) {
        phase1.pendingRounds.remove(round)
      }
    }
    if (!phase1.pendingRounds.isEmpty) {
      return None
    }

    // Stop our timers.
    phase1.resendPhase1as.stop()

    // Compute the largest persistedWatermark. We should not propose any values
    // for entries less than maxPersistedWatermark.
    val maxPersistedWatermark =
      phase1.phase1bs.values.map(_.persistedWatermark).max

    // If persistedWatermark is larger than chosenWatermark, then there are
    // some values that were persisted that we don't know about. For now, we
    // simply ignore them. In a more complete implementation, we maybe would
    // let the replicas know that the values were chosen.
    chosenWatermark = Math.max(chosenWatermark, maxPersistedWatermark)

    // Find the largest slot with a vote, or -1 if there are no votes.
    val slotInfos = phase1.phase1bs.values.flatMap(_.info)
    val maxSlot = if (slotInfos.isEmpty) {
      -1
    } else {
      slotInfos.map(_.slot).max
    }

    // Now, we iterate from chosenWatermark to maxSlot proposing safe
    // values to the acceptors to fill in the log.
    val values = mutable.SortedMap[Slot, CommandOrNoop]()
    for (slot <- chosenWatermark to maxSlot) {
      values(slot) = safeValue(phase1.phase1bs.values, slot)
    }
    Some(values)
  }

  def processPhase2b(phase2: Phase2, phase2b: Phase2b): Phase2 = {
    // Ignore messages from stale rounds.
    if (phase2b.round != phase2.round) {
      logger.debug(
        s"A leader received a Phase2b message in round ${phase2b.round} " +
          s"but is in round ${phase2.round}. The Phase2b is being ignored."
      )
      metrics.stalePhase2bsTotal.inc()
      return phase2
    }

    // Ignore messages for slots that have already been chosen.
    if (phase2b.slot < chosenWatermark ||
        phase2.chosen.contains(phase2b.slot)) {
      logger.debug(
        s"A leader received a Phase2b message in slot ${phase2b.slot} " +
          s"but that slot has already been chosen. The Phase2b message " +
          s"is being ignored."
      )
      metrics.phase2bAlreadyChosenTotal.inc()
      return phase2
    }

    // If the replica tells us that the command has already been persisted,
    // then we can proceed right away to clearing our metadata.
    if (phase2b.persisted) {
      // In a more complete implementation, we might inform the replicas that
      // some value was chosen here, but we don't know what it is. Here, we
      // just don't tell them anything. More likely than not, they already know
      // the value. They will also recover if they don't know it.
    } else {
      // Otherwise, we wait until we have a write quorum.
      val phase2bs = phase2.phase2bs(phase2b.slot)
      phase2bs(phase2b.acceptorIndex) = phase2b

      if (!phase2.quorumSystem.isWriteQuorum(phase2bs.keys.toSet)) {
        return phase2
      }

      // Inform the replicas that the value has been chosen.
      for (replica <- replicas) {
        replica.send(
          ReplicaInbound().withChosen(
            Chosen(slot = phase2b.slot, value = phase2.values(phase2b.slot))
          )
        )
      }
    }

    // Clear our metadata.
    phase2.values.remove(phase2b.slot)
    phase2.phase2bs.remove(phase2b.slot)

    // Update our chosen watermark.
    phase2.chosen += phase2b.slot
    val oldChosenWatermark = chosenWatermark
    while (phase2.chosen.contains(chosenWatermark)) {
      phase2.chosen.remove(chosenWatermark)
      chosenWatermark += 1
    }

    // Otherwise, we are waiting on at least one value to get chosen. If
    // we've updated the chosenWatermark, then we reset the timer.
    if (oldChosenWatermark != chosenWatermark) {
      phase2.resendPhase2as.reset()
    }

    // Broadcast chosenWatermark to other leaders, if needed.
    var numChosenSinceLastWatermarkSend =
      phase2.numChosenSinceLastWatermarkSend + 1
    if (numChosenSinceLastWatermarkSend >=
          options.sendChosenWatermarkEveryN) {
      for (leader <- otherLeaders) {
        leader.send(
          LeaderInbound().withChosenWatermark(
            ChosenWatermark(watermark = chosenWatermark)
          )
        )
      }
      numChosenSinceLastWatermarkSend = 0
    }

    // Check if we're now ready to send garbage collect commands.
    phase2.gc match {
      case WaitingForLargerChosenWatermark(_, maxSlot)
          if chosenWatermark > maxSlot =>
        val garbageCollect = GarbageCollect(matchmakerConfiguration =
                                              matchmakerConfiguration,
                                            gcWatermark = phase2.round)
        matchmakerConfiguration.matchmakerIndex.foreach(
          matchmakers(_).send(
            MatchmakerInbound().withGarbageCollect(garbageCollect)
          )
        )
        return phase2.copy(
          gc = GarbageCollecting(
            gcWatermark = phase2.round,
            matchmakerConfiguration = matchmakerConfiguration,
            garbageCollectAcks = mutable.Set(),
            resendGarbageCollects =
              makeResendGarbageCollectsTimer(garbageCollect)
          ),
          numChosenSinceLastWatermarkSend = numChosenSinceLastWatermarkSend
        )
      case _ =>
        return phase2.copy(
          numChosenSinceLastWatermarkSend = numChosenSinceLastWatermarkSend
        )
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import LeaderInbound.Request

    val label =
      inbound.request match {
        case Request.Phase1B(_)           => "Phase1b"
        case Request.ClientRequest(_)     => "ClientRequest"
        case Request.Phase2B(_)           => "Phase2b"
        case Request.Chosen(_)            => "Chosen"
        case Request.Reconfigure(_)       => "Reconfigure"
        case Request.LeaderInfoRequest(_) => "LeaderInfoRequest"
        case Request.Nack(_)              => "Nack"
        case Request.Recover(_)           => "Recover"
        case Request.Die(_)               => "Die"
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1B(r)           => handlePhase1b(src, r)
        case Request.ClientRequest(r)     => handleClientRequest(src, r)
        case Request.Phase2B(r)           => handlePhase2b(src, r)
        case Request.Chosen(r)            => handleChosen(src, r)
        case Request.Reconfigure(r)       => handleReconfigure(src, r)
        case Request.LeaderInfoRequest(r) => handleLeaderInfoRequest(src, r)
        case Request.Nack(r)              => handleNack(src, r)
        case Request.Recover(r)           => handleRecover(src, r)
        case Request.Die(r)               => handleDie(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    // state match {
    //   case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
    //       _: Phase2 | _: Phase2Matchmaking | _: Phase22 =>
    //     logger.debug(
    //       s"Leader received a Phase1b message but is not in Phase1. Its " +
    //         s"state is $state. The Phase1b message is being ignored."
    //     )
    //
    //   case phase1: Phase1 =>
    //     processPhase1b(phase1, phase1b) match {
    //       case None =>
    //         // Do nothing.
    //         {}
    //
    //       case Some(values) =>
    //         // Proposer the values.
    //         val phase2bs =
    //           mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]()
    //         for ((slot, value) <- values) {
    //           phase2bs(slot) = mutable.Map()
    //           val phase2a =
    //             Phase2a(slot = slot, round = phase1.round, value = value)
    //           for (index <- phase1.quorumSystem.randomWriteQuorum()) {
    //             acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
    //           }
    //         }
    //
    //         metrics.phase1NumEntries.observe(values.size)
    //
    //         // We've filled in every slot up to and including maxSlot, so the
    //         // next slot is maxSlot + 1 (or chosenWatermark if maxSlot + 1 is
    //         // smaller).
    //         val maxSlot = if (values.size == 0) {
    //           -1
    //         } else {
    //           values.lastKey
    //         }
    //         val nextSlot = Math.max(chosenWatermark, maxSlot + 1)
    //
    //         // Kick off GC by sending ExecutedWatermarkRequests to the replicas.
    //         replicas.foreach(
    //           _.send(
    //             ReplicaInbound()
    //               .withExecutedWatermarkRequest(ExecutedWatermarkRequest())
    //           )
    //         )
    //
    //         // Update our state.
    //         val phase2 = Phase2(
    //           round = phase1.round,
    //           nextSlot = nextSlot,
    //           quorumSystem = phase1.quorumSystem,
    //           values = values,
    //           phase2bs = phase2bs,
    //           chosen = mutable.Set(),
    //           numChosenSinceLastWatermarkSend = 0,
    //           resendPhase2as = makeResendPhase2asTimer(),
    //           gc = QueryingReplicas(
    //             chosenWatermark = chosenWatermark,
    //             maxSlot = maxSlot,
    //             executedWatermarkReplies = mutable.Set(),
    //             resendExecutedWatermarkRequests =
    //               makeResendExecutedWatermarkRequestsTimer()
    //           )
    //         )
    //         state = phase2
    //
    //         // Process any pending client requests.
    //         for (clientRequest <- phase1.pendingClientRequests) {
    //           processClientRequest(phase2, clientRequest)
    //         }
    //     }
    //
    //   case phase212: Phase212 =>
    //     // Note that there can't be any pending requests to process.
    //     logger.checkEq(phase212.newPhase1.pendingClientRequests.size, 0)
    //
    //     processPhase1b(phase212.newPhase1, phase1b) match {
    //       case None =>
    //         // Do nothing.
    //         {}
    //
    //       case Some(values) =>
    //         val maxSlot = if (values.size == 0) {
    //           -1
    //         } else {
    //           values.lastKey
    //         }
    //         logger.checkLt(maxSlot, phase212.oldPhase2.nextSlot)
    //
    //         // Propose values to the acceptors in entries [chosenWatermark,
    //         // maxSlot].
    //         for ((slot, value) <- values) {
    //           logger.check(!phase212.newPhase2.phase2bs.contains(slot))
    //           phase212.newPhase2.phase2bs(slot) = mutable.Map()
    //           phase212.newPhase2.values(slot) = value
    //           val phase2a = Phase2a(slot = slot,
    //                                 round = phase212.newPhase2.round,
    //                                 value = value)
    //           for (index <- phase212.newPhase2.quorumSystem
    //                  .randomWriteQuorum()) {
    //             acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
    //           }
    //         }
    //
    //         // Propose values to the acceptors in entries [maxSlot + 1,
    //         // oldPhase2.nextSlot). Typically, this range is subsumed by the
    //         // previous. maxSlot may be -1, if no votes were received. In this
    //         // case, we use chosenWatermark instead.
    //         for (slot <- Math.max(maxSlot + 1, chosenWatermark) until
    //                phase212.oldPhase2.nextSlot) {
    //           logger.check(!phase212.newPhase2.phase2bs.contains(slot))
    //           phase212.newPhase2.phase2bs(slot) = mutable.Map()
    //           phase212.newPhase2.values(slot) = CommandOrNoop().withNoop(Noop())
    //           val phase2a = Phase2a(slot = slot,
    //                                 round = phase212.newPhase2.round,
    //                                 value = CommandOrNoop().withNoop(Noop()))
    //           for (index <- phase212.newPhase2.quorumSystem
    //                  .randomWriteQuorum()) {
    //             acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
    //           }
    //         }
    //
    //         metrics.phase1NumEntries.observe(
    //           values.size +
    //             (Math.max(maxSlot + 1, chosenWatermark) until
    //               phase212.oldPhase2.nextSlot).size
    //         )
    //
    //         // At this point, we either transition to Phase22 or Phase2,
    //         // depending on whether or not the old Phase 2 has finished
    //         // choosing all of its values.
    //         if (chosenWatermark >= phase212.oldPhase2.nextSlot) {
    //           // Kick off GC by sending ExecutedWatermarkRequests to the
    //           // replicas.
    //           replicas.foreach(
    //             _.send(
    //               ReplicaInbound()
    //                 .withExecutedWatermarkRequest(ExecutedWatermarkRequest())
    //             )
    //           )
    //
    //           // Stop timers.
    //           stopTimers(phase212.oldPhase2)
    //
    //           // Update metrics.
    //           metrics.reconfigurationLatency
    //             .labels("Phase212 to Phase2")
    //             .observe(
    //               (System.nanoTime - phase212.startNanos).toDouble / 1000000
    //             )
    //
    //           // Update our state.
    //           state = phase212.newPhase2.copy(
    //             gc = QueryingReplicas(
    //               chosenWatermark = chosenWatermark,
    //               maxSlot = maxSlot,
    //               executedWatermarkReplies = mutable.Set(),
    //               resendExecutedWatermarkRequests =
    //                 makeResendExecutedWatermarkRequestsTimer()
    //             )
    //           )
    //         } else {
    //           val startNanos = System.nanoTime
    //           metrics.reconfigurationLatency
    //             .labels("Phase212 to Phase22")
    //             .observe(
    //               (startNanos - phase212.startNanos).toDouble / 1000000
    //             )
    //
    //           state = Phase22(
    //             oldPhase2 = phase212.oldPhase2,
    //             newPhase2 = phase212.newPhase2,
    //             startNanos = startNanos
    //           )
    //         }
    //     }
    // }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    state match {
      case _: Inactive =>
        // If we're not the active leader but receive a request from a client,
        // then we send back a NotLeader message to let them know that they've
        // contacted the wrong leader.
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(ClientInbound().withNotLeader(NotLeader()))

      case active: Active =>
        // We need to iterate over the chunks and find the first one that is in
        // Phase 1 or is in Phase 2 with vacant slots. Note that a Phase 2
        // chunk may have already filled all of its slots. In this case, we
        // obviously cannot propose a value in the chunk.
        for (chunk <- active.chunks) {
          chunk.phase match {
            case phase1: Phase1 =>
              // We'll process this request when this chunk finishes Phase 1.
              phase1.pendingClientRequests += clientRequest
              return

            case phase2: Phase2 =>
              phase2.nextSlot match {
                case None =>
                  // This chunk is full, we do nothing and move on to the next
                  // chunk.
                  ()

                case Some(nextSlot) =>
                  // Send Phase2as to a write quorum.
                  val value = Value().withCommand(clientRequest.command)
                  val phase2a = Phase2a(slot = nextSlot,
                                        round = active.round,
                                        value = value)
                  for (index <- chunk.quorumSystem.randomWriteQuorum()) {
                    acceptors(index).send(
                      AcceptorInbound().withPhase2A(phase2a)
                    )
                  }

                  // Update our metadata.
                  logger.check(!phase2.values.contains(nextSlot))
                  phase2.values(nextSlot) = value
                  phase2.phase2bs(nextSlot) = mutable.Map()
                  phase2.nextSlot = chunk.lastSlot match {
                    case None => Some(nextSlot + 1)
                    case Some(lastSlot) =>
                      if (nextSlot == lastSlot) {
                        None
                      } else {
                        Some(nextSlot + 1)
                      }
                  }
              }
          }
        }

        logger.fatal(
          "No eligible chunks found. This should be impossible. There must " +
            "be a bug."
        )
    }
  }

  private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    state match {
      case _: Inactive =>
        logger.debug(
          s"Leader received a Phase2b message but is Inactive. The Phase2b " +
            s"message is being ignored."
        )

      case active: Active =>
        // Ignore messages from stale rounds.
        if (phase2b.round != active.round) {
          logger.debug(
            s"A leader received a Phase2b message in round ${phase2b.round} " +
              s"but is in round ${active.round}. The Phase2b is being ignored."
          )
          return
        }

        // Ignore messages for slots that have already been chosen.
        if (phase2b.slot < chosenWatermark || log.get(phase2b.slot).isDefined) {
          logger.debug(
            s"A leader received a Phase2b message in slot ${phase2b.slot} " +
              s"but that slot has already been chosen. The Phase2b message " +
              s"is being ignored."
          )
          return
        }

        getChunk(active.chunks, phase2b.slot) match {
          case None =>
            logger.debug(
              s"Leader received a Phase2b message in slot ${phase2b.slot}, " +
                s"but has no matching chunk. The Phase2b message must be " +
                s"stale. It is being ignored."
            )

          case Some(chunk) =>
            chunk.phase match {
              case phase1: Phase1 =>
                logger.fatal(
                  s"Leader received a Phase2b message but is in Phase1. The " +
                    s"Phase2b message is being ignored."
                )

              case phase2: Phase2 =>
                // Wait until we have a write quorum.
                val phase2bs = phase2.phase2bs(phase2b.slot)
                phase2bs(phase2b.acceptorIndex) = phase2b
                if (!chunk.quorumSystem.isWriteQuorum(phase2bs.keys.toSet)) {
                  return
                }

                // Inform the replicas and other leaders that the value has
                // been chosen.
                val value = phase2.values(phase2b.slot)
                val chosen = Chosen(slot = phase2b.slot, value = value)
                replicas.foreach(_.send(ReplicaInbound().withChosen(chosen)))
                otherLeaders.foreach(_.send(LeaderInbound().withChosen(chosen)))

                // Update our metadata.
                phase2.values.remove(phase2b.slot)
                phase2.phase2bs.remove(phase2b.slot)
                val configurations = choose(phase2b.slot, value)
                // TODO(mwhittaker): Process new configurations.
                ???
            }
        }
    }
  }

  // put in log
  // increase chosen watermark
  // if we encounter any reconfigurations, we need to make new chunks
  // if we make new chunk, we need to run phase1 in that chunk and update the old chunk

  private def handleChosen(src: Transport#Address, chosen: Chosen): Unit = {
    state match {
      case _: Inactive =>
        choose(chosen.slot, chosen.value)

      case _: Active =>
        // An active leader received a Chosen message. An active leader doesn't
        // send Chosen messages to istelf, so this message must be stale or be
        // from another concurrently active leader. Both cases are rare, so we
        // simply ignore the message. If we do decide to process the message,
        // things are more complicated.
        metrics.activeLeaderReceivedChosenTotal.inc()
    }
  }

  private def handleReconfigure(
      src: Transport#Address,
      reconfigure: Reconfigure
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleLeaderInfoRequest(
      src: Transport#Address,
      leaderInfoRequest: LeaderInfoRequest
  ): Unit = {
    // state match {
    //   case _: Inactive =>
    //   // We're inactive, so we ignore the leader info request. The active
    //   // leader will respond to the request.
    //
    //   case _: Matchmaking | _: WaitingForNewMatchmakers | _: Phase1 |
    //       _: Phase2 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
    //     val client = chan[Client[Transport]](src, Client.serializer)
    //     client.send(
    //       ClientInbound().withLeaderInfoReply(
    //         LeaderInfoReply(round = getRound(state))
    //       )
    //     )
    // }
  }

  // TODO(mwhittaker): If we receive a nack, we perform a leader change. When
  // we perform this leader change, we lose all the pending commands. We might
  // want to leader change but put all the pending commands in the pending
  // list. This is not perfect though since the other leaders may also have
  // pending commands that we don't know about. Ultimately, we need good client
  // resend periods, which is not ideal, but it is what it is.
  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    // val smallerRound =
    //   state match {
    //     case inactive: Inactive =>
    //       inactive.round
    //     case matchmaking: Matchmaking =>
    //       matchmaking.round
    //     case waiting: WaitingForNewMatchmakers =>
    //       waiting.round
    //     case phase1: Phase1 =>
    //       phase1.round
    //     case phase2: Phase2 =>
    //       phase2.round
    //     case phase2Matchmaking: Phase2Matchmaking =>
    //       phase2Matchmaking.phase2.round
    //     case phase212: Phase212 =>
    //       phase212.oldPhase2.round
    //     case phase22: Phase22 =>
    //       phase22.oldPhase2.round
    //   }
    //
    // if (nack.round < smallerRound) {
    //   logger.debug(
    //     s"A Leader received an AcceptorNack message with round " +
    //       s"${nack.round} but is already in round ${smallerRound}. " +
    //       s"The Nack is being ignored."
    //   )
    //   metrics.staleAcceptorNackTotal.inc()
    //   return
    // }
    //
    // state match {
    //   case inactive: Inactive =>
    //     state = inactive.copy(round = nack.round)
    //
    //   case _: Matchmaking | _: WaitingForNewMatchmakers =>
    //     logger.debug(
    //       s"Leader received an AcceptorNack with round ${nack.round} but is " +
    //         s"not in Phase 1 or 2. Its state is $state. The nack is being " +
    //         s"ignored."
    //     )
    //
    //   case _: Phase1 | _: Phase2 | _: Phase2Matchmaking | _: Phase212 |
    //       _: Phase22 =>
    //     val newRound = roundSystem.nextClassicRound(
    //       leaderIndex = index,
    //       round = Math.max(nack.round, getRound(state))
    //     )
    //     becomeLeader(newRound)
    // }
  }

  // This form of recovery is super heavy-handed. We really shouldn't be
  // running a complete leader change to recover a single value. However,
  // recovery should be extremeley rare, so we're ok.
  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    // state match {
    //   case _: Inactive =>
    //     // Do nothing. The active leader will recover.
    //     {}
    //
    //   case _: Matchmaking | _: WaitingForNewMatchmakers | _: Phase1 |
    //       _: Phase2 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
    //     // If our chosen watermark is larger than the slot we're trying to
    //     // recover, then we won't get the value chosen again. We have to lower
    //     // our watermark so that the protocol gets the value chosen again.
    //     if (chosenWatermark > recover.slot) {
    //       chosenWatermark = recover.slot
    //     }
    //
    //     // Leader change to make sure the slot is chosen.
    //     becomeLeader(getNextRound(state))
    // }
  }

  private def handleDie(src: Transport#Address, die: Die): Unit = {
    logger.fatal("Die!")
  }

  // private def handleForceReconfiguration(
  //     src: Transport#Address,
  //     forceReconfiguration: ForceReconfiguration
  // ): Unit = {
  // val quorumSystem = new SimpleMajority(
  //   forceReconfiguration.acceptorIndex.toSet,
  //   seed
  // )
  // becomeIIPlusOneLeader(quorumSystem, QuorumSystem.toProto(quorumSystem))
  // }

  // API ///////////////////////////////////////////////////////////////////////
  // For the JS frontend.
  def reconfigure(): Unit = {
    // val (qs, qsp) = getRandomQuorumSystem(config.numAcceptors)
    // becomeIIPlusOneLeader(qs, qsp)
  }
}
