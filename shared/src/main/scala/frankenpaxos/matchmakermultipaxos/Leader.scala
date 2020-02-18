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
    .name("matchmakermultipaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakermultipaxos_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val staleMatchRepliesTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_stale_match_replies_total")
    .help("Total number of stale MatchReplies received.")
    .register()

  val stalePhase1bsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_stale_phase1bs_total")
    .help("Total number of stale Phase1bs received.")
    .register()

  val stalePhase2bsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_stale_phase2bs_total")
    .help("Total number of stale Phase2bs received.")
    .register()

  val phase2bAlreadyChosenTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_phase2b_already_chosen_total")
    .help(
      "Total number of Phase2bs received for a slot that was already chosen."
    )
    .register()

  val staleMatchmakerNackTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_stale_matchmaker_nack_total")
    .help("Total number of stale MatchmakerNacks received.")
    .register()

  val staleAcceptorNackTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_stale_acceptor_nack_total")
    .help("Total number of stale AcceptorNacks received.")
    .register()

  val stopBeingLeaderTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_stop_being_leader_total")
    .help("Total number of times a node stops being a leader.")
    .register()

  val becomeLeaderTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_become_leader_total")
    .help("Total number of times a node becomes the leader.")
    .register()

  val resendMatchRequestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_resend_match_requests_total")
    .help("Total number of times the leader resent MatchRequest messages.")
    .register()

  val resendReconfigureTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_resend_reconfigure_total")
    .help("Total number of times the leader resent Reconfigure messages.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_resend_phase1as_total")
    .help("Total number of times the leader resent Phase1a messages.")
    .register()

  val resendPhase2asTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_resend_phase2as_total")
    .help("Total number of times the leader resent Phase2a messages.")
    .register()

  val resendExecutedWatermarkRequestsTotal: Counter = collectors.counter
    .build()
    .name(
      "matchmakermultipaxos_leader_resend_executed_watermark_requests_total"
    )
    .help(
      "Total number of times the leader resent ExecutedWatermarkRequest messages."
    )
    .register()

  val resendPersistedTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_resend_persisted_total")
    .help("Total number of times the leader resent Persisted messages.")
    .register()

  val resendGarbageCollectsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_resend_garbage_collect_total")
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
  type MatchmakerIndex = Int
  type Round = Int
  type Slot = Int

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
  case class Matchmaking(
      round: Round,
      // The matchmakers used to matchmake. If this set of matchmakers is
      // stopped and a new set of matchmakers is elected, then we'll restart
      // Matchmaking in the same round as now, but with the new matchmakers.
      matchmakerConfiguration: MatchmakerConfiguration,
      // The quorum system that this leader intends to use to get values chosen.
      quorumSystem: QuorumSystem[AcceptorIndex],
      quorumSystemProto: QuorumSystemProto,
      // The MatchReplies received from the matchmakers.
      matchReplies: mutable.Map[MatchmakerIndex, MatchReply],
      // Any pending client requests. These client requests will be processed
      // as soon as the leader enters Phase 2.
      pendingClientRequests: mutable.Buffer[ClientRequest],
      // A timer to resend match requests, for liveness.
      resendMatchRequests: Transport#Timer
  ) extends State

  // If we were in the middle of Matchmaking, but the matchmakers were stopped,
  // then we have to contact the reconfigurers and wait for them to tell us
  // about the new matchmakers. When we do learn about the new matchmakers, we
  // revert back to the Matchmaking phase.
  @JSExportAll
  case class WaitingForNewMatchmakers(
      // When we resume matchmaking, we resume in this round.
      round: Round,
      // The matchmakers previously used to matchmake. We send this
      // configuration to the reconfigurers when asking for a new
      // configuration.
      matchmakerConfiguration: MatchmakerConfiguration,
      // The quorum system that this leader intends to use to get values chosen.
      quorumSystem: QuorumSystem[AcceptorIndex],
      quorumSystemProto: QuorumSystemProto,
      // Any pending client requests. These client requests will be processed
      // as soon as the leader enters Phase 2.
      pendingClientRequests: mutable.Buffer[ClientRequest],
      // A timer to resend Reconfigure, for liveness.
      resendReconfigure: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase1(
      round: Round,
      // The quorum system that this leader intends to use to get values chosen.
      quorumSystem: QuorumSystem[AcceptorIndex],
      // The quorum systems returned by the matchmakers for previous rounds.
      previousQuorumSystems: Map[Round, QuorumSystem[AcceptorIndex]],
      // A mapping from an acceptor to the rounds in which it has participated.
      acceptorToRounds: Map[AcceptorIndex, mutable.Set[Round]],
      // The rounds for which we still need to hear back from a read quorum.
      pendingRounds: mutable.Set[Round],
      // The Phase1bs received from the acceptors.
      phase1bs: mutable.Map[AcceptorIndex, Phase1b],
      // Any pending client requests. These client requests will be processed
      // as soon as the leader enters Phase 2.
      pendingClientRequests: mutable.Buffer[ClientRequest],
      // A timer to resend Phase1as, for liveness.
      resendPhase1as: Transport#Timer
  ) extends State

  // When a leader enters phase 2 in round i, it attempts to garbage collect
  // configurations in round less than i. To do so, it does the following:
  //
  //   1. Upon transitioning from phase 1 to phase 2, the leader uses a given
  //      chosenWatermark and a computes a given maxSlot.
  //   2. The leader repeatedly queries the replicas until at least f+1 have
  //      executed commands up through and including chosenWatermark.
  //   3. The leader informs the acceptors that these slots have been persisted
  //      on the replicas.
  //   4. The leader waits for all slots up to and including maxSlot have been
  //      chosen.
  //   5. The leader sends a garbage collect command to the matchmakers and
  //      waits to hear back all the acks.
  @JSExportAll
  sealed trait GarbageCollection

  @JSExportAll
  case class QueryingReplicas(
      // At the end of Phase 1 and at the beginning of Phase 2, entries [0,
      // chosenWatermark) were known to be chosen. Phase2as were sent for
      // entries [chosenWatermark, maxSlot]. Entries (maxSlot, infinity) were
      // known to be empty.
      chosenWatermark: Slot,
      maxSlot: Slot,
      executedWatermarkReplies: mutable.Set[ReplicaIndex],
      resendExecutedWatermarkRequests: Transport#Timer
  ) extends GarbageCollection

  @JSExportAll
  case class PushingToAcceptors(
      // At the end of Phase 1 and at the beginning of Phase 2, entries [0,
      // chosenWatermark) were known to be chosen. Phase2as were sent for
      // entries [chosenWatermark, maxSlot]. Entries (maxSlot, infinity) were
      // known to be empty.
      chosenWatermark: Slot,
      maxSlot: Slot,
      quorumSystem: QuorumSystem[AcceptorIndex],
      persistedAcks: mutable.Set[AcceptorIndex],
      resendPersisted: Transport#Timer
  ) extends GarbageCollection

  @JSExportAll
  case class WaitingForLargerChosenWatermark(
      // At the end of Phase 1 and at the beginning of Phase 2, entries [0,
      // chosenWatermark) were known to be chosen. Phase2as were sent for
      // entries [chosenWatermark, maxSlot]. Entries (maxSlot, infinity) were
      // known to be empty.
      chosenWatermark: Slot,
      maxSlot: Slot
  ) extends GarbageCollection

  @JSExportAll
  case class GarbageCollecting(
      gcWatermark: Round,
      // The matchmakers to which we sent GarbageCollect messages. If a
      // matchmaker reconfiguration happens during the reconfiguration, then we
      // abort the GC.
      matchmakerConfiguration: MatchmakerConfiguration,
      garbageCollectAcks: mutable.Set[MatchmakerIndex],
      resendGarbageCollects: Transport#Timer
  ) extends GarbageCollection

  @JSExportAll
  case object Done extends GarbageCollection

  // During an i/i+1 configuration, we cancel any pending garbage collection
  // being performed during round i. This is not necessary for correctness, but
  // simplifies the implementation. Moreover, it shouldn't have a big impact on
  // performance.
  //
  // Similarly, if a matchmaker reconfiguration happens during garbage
  // collection, we just cancel the garbage collection.
  @JSExportAll
  case object Cancelled extends GarbageCollection

  @JSExportAll
  case class Phase2(
      round: Round,
      // The next free slot in the log. Note that even though we have a
      // nextSlot, we don't actually have a log.
      var nextSlot: Slot,
      // The quorum system that this leader uses to get values chosen.
      quorumSystem: QuorumSystem[AcceptorIndex],
      // The values that the leader is trying to get chosen.
      values: mutable.Map[Slot, CommandOrNoop],
      // The Phase2b responses received from the acceptors.
      phase2bs: mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]],
      // A set of slots larger than `chosenWatermark` that have been chosen.
      chosen: mutable.Set[Slot],
      // The number of commands that have been chosen since the last time the
      // active leader sends its chosen watermark to the other leaders.
      numChosenSinceLastWatermarkSend: Int,
      // A timer to resend Phase2as, for liveness. This timer is reset every
      // time the chosenWatermark is incremented. If the timer is fired, then
      // Phase2as are only sent for the smallest pending command.
      resendPhase2as: Transport#Timer,
      // See above for a description of these values.
      gc: GarbageCollection
  ) extends State

  // TODO(mwhittaker): Document.
  @JSExportAll
  case class Phase2Matchmaking(
      phase2: Phase2,
      matchmaking: Matchmaking
  ) extends State

  // TODO(mwhittaker): Document.
  @JSExportAll
  case class Phase212(
      oldPhase2: Phase2,
      newPhase1: Phase1,
      newPhase2: Phase2
  ) extends State

  // TODO(mwhittaker): Document.
  @JSExportAll
  case class Phase22(
      oldPhase2: Phase2,
      newPhase2: Phase2
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

  // Reconfigurer channels.
  private val reconfigurers: Seq[Chan[Reconfigurer[Transport]]] =
    for (a <- config.reconfigurerAddresses)
      yield chan[Reconfigurer[Transport]](a, Reconfigurer.serializer)

  // Matchmaker channels.
  private val matchmakers: Seq[Chan[Matchmaker[Transport]]] =
    for (a <- config.matchmakerAddresses)
      yield chan[Matchmaker[Transport]](a, Matchmaker.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (a <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](a, Acceptor.serializer)

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

  // TODO(mwhittaker): Use a ClassicStutteredRound system and pass in the
  // stutter as a flag.
  // For simplicity, we use a round robin round system for the leaders.
  private val roundSystem = new RoundSystem.ClassicStutteredRoundRobin(
    n = config.numLeaders,
    stutterLength = options.stutter
  )

  // Every slot less than chosenWatermark has been chosen. The active leader
  // periodically sends its chosenWatermarks to the other leaders.
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
      becomeLeader(getNextRound(state))
    } else {
      stopBeingLeader()
    }
  })

  // The latest matchmakerConfiguration that this leader knows about.
  @JSExport
  protected var matchmakerConfiguration: MatchmakerConfiguration =
    MatchmakerConfiguration(
      epoch = 0,
      reconfigurerIndex = -1,
      matchmakerIndex = 0 until (2 * config.f + 1)
    )

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    val (qs, qsp) = getRandomQuorumSystem(config.numAcceptors)
    startMatchmaking(round = 0,
                     pendingClientRequests = mutable.Buffer(),
                     qs,
                     qsp)
  } else {
    Inactive(round = -1)
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendMatchRequestsTimer(
      matchRequest: MatchRequest,
      matchmakerIndices: Set[Int]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendMatchRequests",
      options.resendMatchRequestsPeriod,
      () => {
        metrics.resendMatchRequestsTotal.inc()
        for (index <- matchmakerIndices) {
          matchmakers(index).send(
            MatchmakerInbound().withMatchRequest(matchRequest)
          )
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendReconfigureTimer(
      reconfigure: Reconfigure
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendReconfigure",
      options.resendReconfigurePeriod,
      () => {
        metrics.resendReconfigureTotal.inc()
        val reconfigurer = reconfigurers(rand.nextInt(reconfigurers.size))
        reconfigurer.send(ReconfigurerInbound().withReconfigure(reconfigure))
        t.start()
      }
    )
    t.start()
    t
  }

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

        phase2.values.get(chosenWatermark) match {
          case Some(value) =>
            val phase2a =
              Phase2a(slot = chosenWatermark,
                      round = getRound(state),
                      value = value)
            for (index <- phase2.quorumSystem.nodes) {
              acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
            }

          case None =>
            logger.debug(
              s"The resendPhase2as timer was triggered but there is no " +
                s"pending value for slot $chosenWatermark (the chosen " +
                s"watermark)."
            )
        }

        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendExecutedWatermarkRequestsTimer(): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendExecutedWatermarkRequests",
      options.resendExecutedWatermarkRequestsPeriod,
      () => {
        metrics.resendExecutedWatermarkRequestsTotal.inc()
        replicas.foreach(
          _.send(
            ReplicaInbound()
              .withExecutedWatermarkRequest(ExecutedWatermarkRequest())
          )
        )
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPersistedTimer(
      persisted: Persisted,
      indices: Set[AcceptorIndex]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPersisted",
      options.resendPersistedPeriod,
      () => {
        metrics.resendPersistedTotal.inc()
        indices.foreach(
          acceptors(_).send(AcceptorInbound().withPersisted(persisted))
        )
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendGarbageCollectsTimer(
      garbageCollect: GarbageCollect
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendGarbageCollects",
      options.resendGarbageCollectsPeriod,
      () => {
        metrics.resendGarbageCollectsTotal.inc()
        matchmakers.foreach(
          _.send(MatchmakerInbound().withGarbageCollect(garbageCollect))
        )
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

  // Returns the current round. In the case of a state with multiple active
  // rounds, the larger round is returned.
  private def getRound(state: State): Round = {
    state match {
      case inactive: Inactive =>
        inactive.round
      case matchmaking: Matchmaking =>
        matchmaking.round
      case waiting: WaitingForNewMatchmakers =>
        waiting.round
      case phase1: Phase1 =>
        phase1.round
      case phase2: Phase2 =>
        phase2.round
      case phase2Matchmaking: Phase2Matchmaking =>
        phase2Matchmaking.matchmaking.round
      case phase212: Phase212 =>
        phase212.newPhase2.round
      case phase22: Phase22 =>
        phase22.newPhase2.round
    }
  }

  // Returns the smallest round owned by this leader that is larger than any
  // round in `state`.
  private def getNextRound(state: State): Round =
    roundSystem.nextClassicRound(leaderIndex = index, round = getRound(state))

  private def stopGcTimers(gc: GarbageCollection): Unit = {
    gc match {
      case gc: QueryingReplicas =>
        gc.resendExecutedWatermarkRequests.stop()
      case gc: PushingToAcceptors =>
        gc.resendPersisted.stop()
      case gc: WaitingForLargerChosenWatermark =>
      case gc: GarbageCollecting =>
        gc.resendGarbageCollects.stop()
      case Done      =>
      case Cancelled =>
    }
  }

  private def stopTimers(state: State): Unit = {
    state match {
      case _: Inactive => ()
      case matchmaking: Matchmaking =>
        matchmaking.resendMatchRequests.stop()
      case waitingForReconfigure: WaitingForNewMatchmakers =>
        waitingForReconfigure.resendReconfigure.stop()
      case phase1: Phase1 =>
        phase1.resendPhase1as.stop()
      case phase2: Phase2 =>
        phase2.resendPhase2as.stop()
        stopGcTimers(phase2.gc)
      case Phase2Matchmaking(phase2, matchmaking) =>
        phase2.resendPhase2as.stop()
        stopGcTimers(phase2.gc)
        matchmaking.resendMatchRequests.stop()
      case Phase212(oldPhase2, newPhase1, newPhase2) =>
        oldPhase2.resendPhase2as.stop()
        stopGcTimers(oldPhase2.gc)
        newPhase1.resendPhase1as.stop()
        newPhase2.resendPhase2as.stop()
        stopGcTimers(newPhase2.gc)
      case Phase22(oldPhase2, newPhase2) =>
        oldPhase2.resendPhase2as.stop()
        stopGcTimers(oldPhase2.gc)
        newPhase2.resendPhase2as.stop()
        stopGcTimers(newPhase2.gc)
    }
  }

  private def pendingClientRequests(
      state: State
  ): mutable.Buffer[ClientRequest] = {
    state match {
      case _: Inactive | _: Phase2 | _: Phase2Matchmaking | _: Phase212 |
          _: Phase22 =>
        mutable.Buffer()
      case matchmaking: Matchmaking =>
        matchmaking.pendingClientRequests
      case waitingForReconfigure: WaitingForNewMatchmakers =>
        waitingForReconfigure.pendingClientRequests
      case phase1: Phase1 =>
        phase1.pendingClientRequests
    }
  }

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

  // Given acceptors a_0, ..., a_{n-1}, randomly create a quorum system.
  //
  // TODO(mwhittaker): For now, we select a quorum system at random because it
  // is the simplest thing to do. In full generality, we should pass in the
  // policy by which we choose quorums.
  private def getRandomQuorumSystem(
      n: Int
  ): (QuorumSystem[AcceptorIndex], QuorumSystemProto) = {
    val seed = rand.nextLong()

    // We randomly pick between simple majority quorums and unanimous write
    // quorums. The thing is, though, that we can only use simple majority
    // quorums if we have at least 2*f+1 acceptors.
    if (config.numAcceptors >= 2 * config.f + 1 && rand.nextBoolean()) {
      val quorumSystem = new SimpleMajority(
        rand
          .shuffle(List() ++ (0 until n))
          .take(2 * config.f + 1)
          .toSet,
        seed
      )
      (quorumSystem, QuorumSystem.toProto(quorumSystem))
    } else {
      val quorumSystem = new UnanimousWrites(
        rand.shuffle(List() ++ (0 until n)).take(config.f + 1).toSet,
        seed
      )
      (quorumSystem, QuorumSystem.toProto(quorumSystem))
    }
  }

  // Given matchmakers a_0, ..., a_{n-1}, randomly choose a subset of 2f+1 them.
  //
  // TODO(mwhittaker): For now, we select a set of matchmakers at random
  // because it is the simplest thing to do. In full generality, we should pass
  // in the policy by which we choose quorums.
  private def getRandomMatchmakers(n: Int): Set[MatchmakerIndex] =
    rand.shuffle(List() ++ (0 until n)).take(2 * config.f + 1).toSet

  private def startMatchmaking(
      round: Round,
      pendingClientRequests: mutable.Buffer[ClientRequest],
      quorumSystem: QuorumSystem[AcceptorIndex],
      quorumSystemProto: QuorumSystemProto
  ): Matchmaking = {
    // Send MatchRequests to the matchmakers.
    val matchRequest = MatchRequest(
      matchmakerConfiguration = matchmakerConfiguration,
      configuration =
        Configuration(round = round, quorumSystem = quorumSystemProto)
    )
    for (index <- matchmakerConfiguration.matchmakerIndex) {
      matchmakers(index).send(
        MatchmakerInbound().withMatchRequest(matchRequest)
      )
    }

    Matchmaking(
      round = round,
      matchmakerConfiguration = matchmakerConfiguration,
      quorumSystem = quorumSystem,
      quorumSystemProto = quorumSystemProto,
      matchReplies = mutable.Map(),
      pendingClientRequests = pendingClientRequests,
      resendMatchRequests = makeResendMatchRequestsTimer(
        matchRequest,
        matchmakerConfiguration.matchmakerIndex.toSet
      )
    )
  }

  private def processClientRequest(
      phase2: Phase2,
      clientRequest: ClientRequest
  ): Unit = {
    // Send Phase2as to a write quorum.
    val slot = phase2.nextSlot
    phase2.nextSlot += 1
    val value = CommandOrNoop().withCommand(clientRequest.command)
    val phase2a = Phase2a(slot = slot, round = phase2.round, value = value)
    for (index <- phase2.quorumSystem.randomWriteQuorum()) {
      acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
    }

    // Update our metadata.
    logger.check(!phase2.values.contains(slot))
    phase2.values(slot) = value
    phase2.phase2bs(slot) = mutable.Map()
  }

  private def stopBeingLeader(): Unit = {
    metrics.stopBeingLeaderTotal.inc()
    stopTimers(state)
    state = Inactive(getRound(state))
  }

  private def becomeLeader(newRound: Round): Unit = {
    logger.checkGt(newRound, getRound(state))
    logger.check(roundSystem.leader(newRound) == index)
    metrics.becomeLeaderTotal.inc()

    stopTimers(state)
    val (qs, qsp) = getRandomQuorumSystem(config.numAcceptors)
    state = startMatchmaking(newRound, pendingClientRequests(state), qs, qsp)
  }

  private def becomeIIPlusOneLeader(): Unit = {
    state match {
      case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
          _: Phase1 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        becomeLeader(getNextRound(state))

      case phase2: Phase2 =>
        if (roundSystem.leader(phase2.round + 1) == index) {
          val (qs, qsp) = getRandomQuorumSystem(config.numAcceptors)
          val matchmaking = startMatchmaking(
            round = phase2.round + 1,
            pendingClientRequests = mutable.Buffer(),
            qs,
            qsp
          )
          state = Phase2Matchmaking(phase2, matchmaking)
        } else {
          becomeLeader(getNextRound(state))
        }
    }
  }

  // processMatchReply processes a MatchReply. There are three possibilities
  // when processing a MatchReply.
  //
  //   1. The Matchmaking phase is not over yet. In this case,
  //      processMatchReply returns None.
  //   2. The Matchmaking phase is over and there are not any previous rounds
  //      to intersect with. In this case, processMatchReply returns a Phase2.
  //      We can proceed directly to Phase 2.
  //   3. The Matchmaking phase is over and there are any previous rounds
  //      to intersect with. In this case, processMatchReply sends Phase1a
  //      messages and returns a Phase1.
  def processMatchReply(
      matchmaking: Matchmaking,
      matchReply: MatchReply
  ): Option[Either[Phase1, Phase2]] = {
    // Ignore stale epochs.
    if (matchReply.epoch != matchmaking.matchmakerConfiguration.epoch) {
      logger.debug(
        s"Leader received a MatchReply in epoch ${matchReply.epoch} but " +
          s"is already in epoch " +
          s"${matchmaking.matchmakerConfiguration.epoch}. The MatchReply " +
          s"is being ignored."
      )
      return None
    }

    // Ignore stale rounds.
    if (matchReply.round != matchmaking.round) {
      logger.debug(
        s"Leader received a MatchReply in round ${matchReply.round} but " +
          s"is already in round ${matchmaking.round}. The MatchReply is " +
          s"being ignored."
      )
      metrics.staleMatchRepliesTotal.inc()
      // We can't receive MatchReplies from the future.
      logger.checkLt(matchReply.round, matchmaking.round)
      return None
    }

    // Wait until we have a quorum of responses.
    matchmaking.matchReplies(matchReply.matchmakerIndex) = matchReply
    if (matchmaking.matchReplies.size < config.quorumSize) {
      return None
    }

    // Stop our timers.
    matchmaking.resendMatchRequests.stop()

    // Compute the following:
    //
    //   - pendingRounds: the set of all rounds for which some quorum
    //     system was returned. We need to intersect the quorum systems in
    //     these rounds.
    //   - previousQuorumSystems: the quorum system for every round in
    //     pendingRounds.
    //   - acceptorIndices: the union of a read quorum for every round in
    //     `pendingRounds`. These are the acceptors to which we send a
    //     Phase 1a message.
    //
    //     TODO(mwhittaker): We only need to send to enough acceptors to
    //     form a read quorum for every round in `pendingRounds`. I think
    //     this might be some complicated NP complete problem or something,
    //     so we just take a union of a read set from every quorum. There
    //     might be a better way to do this.
    //   - acceptorToRounds: an index mapping each acceptor's index to the
    //     set of rounds that it appears in. When we receive a Phase 1b
    //     from an acceptor, this index allows us to quickly figure out
    //     which rounds to update.
    //
    // For example, imagine a leader receives the following two responses
    // from the matchmakers with all quorums being unanimous write quorums:
    //
    //     0   1   2   3
    //   +---+---+---+---+
    //   |0,1|   |   |0,4|
    //   +---+---+---+---+
    //   +---+---+---+---+
    //   |   |2,3|   |0,4|
    //   +---+---+---+---+
    //
    // Then,
    //
    //   - pendingRounds = {0, 1, 3}
    //   - previousQuorumSystems = {0 -> [0, 1], 1 -> [2, 3], 3 -> [0, 4]}
    //   - acceptorIndices = {0, 3, 4}
    //   - acceptorToRounds = {0->[0,3], 1->[0], 2->[1], 3->[1], 4->[3]}
    val pendingRounds = mutable.Set[Round]()
    val previousQuorumSystems =
      mutable.Map[Round, QuorumSystem[AcceptorIndex]]()
    val acceptorIndices = mutable.Set[AcceptorIndex]()
    val acceptorToRounds = mutable.Map[AcceptorIndex, mutable.Set[Round]]()

    val gcWatermark = matchmaking.matchReplies.values.map(_.gcWatermark).max
    for {
      reply <- matchmaking.matchReplies.values
      configuration <- reply.configuration
      if configuration.round >= gcWatermark
    } {
      if (pendingRounds.contains(configuration.round)) {
        // Do nothing. We've already processed this configuration.
      } else {
        pendingRounds += configuration.round
        val quorumSystem = QuorumSystem.fromProto(configuration.quorumSystem)
        previousQuorumSystems(configuration.round) = quorumSystem
        acceptorIndices ++= quorumSystem.randomReadQuorum()

        for (index <- quorumSystem.nodes) {
          acceptorToRounds
            .getOrElseUpdate(index, mutable.Set[Round]())
            .add(configuration.round)
        }
      }
    }

    // If there are no pending rounds, then we're done already! We can skip
    // straight to phase 2. Otherwise, we have to go through phase 1.
    if (pendingRounds.isEmpty) {
      // In this case, we can issue GarbageCollect commands immediately
      // because we no values have been chosen in any log entry in any
      // round less than us. However, we didn't receive any configurations
      // anyway, so there is no need to garbage collect in the first place.
      return Some(
        Right(
          Phase2(
            round = matchmaking.round,
            nextSlot = chosenWatermark,
            quorumSystem = matchmaking.quorumSystem,
            values = mutable.Map(),
            phase2bs = mutable.Map(),
            chosen = mutable.Set(),
            numChosenSinceLastWatermarkSend = 0,
            resendPhase2as = makeResendPhase2asTimer(),
            gc = Done
          )
        )
      )
    } else {
      // Send Phase1as to acceptors.
      val phase1a =
        Phase1a(round = matchmaking.round, chosenWatermark = chosenWatermark)
      for (index <- acceptorIndices) {
        acceptors(index).send(AcceptorInbound().withPhase1A(phase1a))
      }

      // Update our state.
      return Some(
        Left(
          Phase1(
            round = matchmaking.round,
            quorumSystem = matchmaking.quorumSystem,
            previousQuorumSystems = previousQuorumSystems.toMap,
            acceptorToRounds = acceptorToRounds.toMap,
            pendingRounds = pendingRounds,
            phase1bs = mutable.Map(),
            pendingClientRequests = matchmaking.pendingClientRequests,
            resendPhase1as =
              makeResendPhase1asTimer(phase1a, acceptorToRounds.keys.toSet)
          )
        )
      )
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
        case Request.MatchReply(_)             => "MatchReply"
        case Request.Phase1B(_)                => "Phase1b"
        case Request.ClientRequest(_)          => "ClientRequest"
        case Request.Phase2B(_)                => "Phase2b"
        case Request.LeaderInfoRequest(_)      => "LeaderInfoRequest"
        case Request.ChosenWatermark(_)        => "ChosenWatermark"
        case Request.MatchmakerNack(_)         => "MatchmakerNack"
        case Request.AcceptorNack(_)           => "AcceptorNack"
        case Request.Recover(_)                => "Recover"
        case Request.ExecutedWatermarkReply(_) => "ExecutedWatermarkReply"
        case Request.GarbageCollectAck(_)      => "GarbageCollectAck"
        case Request.PersistedAck(_)           => "PersistedAck"
        case Request.Stopped(_)                => "Stopped"
        case Request.MatchChosen(_)            => "MatchChosen"
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.MatchReply(r)        => handleMatchReply(src, r)
        case Request.Phase1B(r)           => handlePhase1b(src, r)
        case Request.ClientRequest(r)     => handleClientRequest(src, r)
        case Request.Phase2B(r)           => handlePhase2b(src, r)
        case Request.LeaderInfoRequest(r) => handleLeaderInfoRequest(src, r)
        case Request.ChosenWatermark(r)   => handleChosenWatermark(src, r)
        case Request.MatchmakerNack(r)    => handleMatchmakerNack(src, r)
        case Request.AcceptorNack(r)      => handleAcceptorNack(src, r)
        case Request.Recover(r)           => handleRecover(src, r)
        case Request.ExecutedWatermarkReply(r) =>
          handleExecutedWatermarkReply(src, r)
        case Request.GarbageCollectAck(r) => handleGarbageCollectAck(src, r)
        case Request.PersistedAck(r)      => handlePersistedAck(src, r)
        case Request.Stopped(r)           => handleStopped(src, r)
        case Request.MatchChosen(r)       => handleMatchChosen(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleMatchReply(
      src: Transport#Address,
      matchReply: MatchReply
  ): Unit = {
    state match {
      case _: Inactive | _: WaitingForNewMatchmakers | _: Phase1 | _: Phase2 |
          _: Phase212 | _: Phase22 =>
        logger.debug(
          s"Leader received a MatchReply but is not currently in the " +
            s"Matchmaking phase (its state is $state). The MatchReply is " +
            s"being ignored."
        )

      case matchmaking: Matchmaking =>
        processMatchReply(matchmaking, matchReply) match {
          case None =>
            // Do nothing
            {}

          case Some(Left(phase1: Phase1)) =>
            state = phase1

          case Some(Right(phase2: Phase2)) =>
            state = phase2
            for (clientRequest <- matchmaking.pendingClientRequests) {
              processClientRequest(phase2, clientRequest)
            }
        }

      case phase2Matchmaking: Phase2Matchmaking =>
        val matchmaking = phase2Matchmaking.matchmaking

        // We shouldn't have any pending client requests, since any pending
        // requests can be served by the Phase 2 in round i. This is important
        // to note because if we did have pending client requests, then we
        // would have to process them when transitioning to a new Phase 2.
        logger.checkEq(matchmaking.pendingClientRequests.size, 0)

        processMatchReply(matchmaking, matchReply) match {
          case None =>
            // Do nothing
            {}

          case Some(Left(phase1: Phase1)) =>
            // Matchmaking is done. We're about to transition to Phase212. We
            // stop any pending GC in the old Phase 2. It's unlikely that it's
            // happening anyway, and it simplifies things a bit.
            stopGcTimers(phase2Matchmaking.phase2.gc)

            // Next, we transition to Phase 1 and Phase 2.
            state = Phase212(
              oldPhase2 = phase2Matchmaking.phase2.copy(gc = Cancelled),
              newPhase1 = phase1,
              newPhase2 = Phase2(
                round = matchmaking.round,
                nextSlot = phase2Matchmaking.phase2.nextSlot,
                quorumSystem = matchmaking.quorumSystem,
                values = mutable.Map(),
                phase2bs = mutable.Map(),
                chosen = mutable.Set(),
                numChosenSinceLastWatermarkSend = 0,
                makeResendPhase2asTimer(),
                // We wait until we enter a stable Phase 2 to perform garbage
                // collection.
                gc = Cancelled
              )
            )

          case Some(Right(_: Phase2)) =>
            logger.fatal(
              s"We transitioned from round i to round i+1, so the " +
                s"Matchmaking phase should return the configuration used " +
                s"in round i. It's impossible to not find any configuration."
            )
        }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    state match {
      case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
          _: Phase2 | _: Phase2Matchmaking | _: Phase22 =>
        logger.debug(
          s"Leader received a Phase1b message but is not in Phase1. Its " +
            s"state is $state. The Phase1b message is being ignored."
        )

      case phase1: Phase1 =>
        processPhase1b(phase1, phase1b) match {
          case None =>
            // Do nothing.
            {}

          case Some(values) =>
            // Proposer the values.
            val phase2bs =
              mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]()
            for ((slot, value) <- values) {
              phase2bs(slot) = mutable.Map()
              val phase2a =
                Phase2a(slot = slot, round = phase1.round, value = value)
              for (index <- phase1.quorumSystem.randomWriteQuorum()) {
                acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
              }
            }

            // We've filled in every slot up to and including maxSlot, so the
            // next slot is maxSlot + 1 (or chosenWatermark if maxSlot + 1 is
            // smaller).
            val maxSlot = if (values.size == 0) {
              -1
            } else {
              values.lastKey
            }
            val nextSlot = Math.max(chosenWatermark, maxSlot + 1)

            // Kick off GC by sending ExecutedWatermarkRequests to the replicas.
            replicas.foreach(
              _.send(
                ReplicaInbound()
                  .withExecutedWatermarkRequest(ExecutedWatermarkRequest())
              )
            )

            // Update our state.
            val phase2 = Phase2(
              round = phase1.round,
              nextSlot = nextSlot,
              quorumSystem = phase1.quorumSystem,
              values = values,
              phase2bs = phase2bs,
              chosen = mutable.Set(),
              numChosenSinceLastWatermarkSend = 0,
              resendPhase2as = makeResendPhase2asTimer(),
              gc = QueryingReplicas(
                chosenWatermark = chosenWatermark,
                maxSlot = maxSlot,
                executedWatermarkReplies = mutable.Set(),
                resendExecutedWatermarkRequests =
                  makeResendExecutedWatermarkRequestsTimer()
              )
            )
            state = phase2

            // Process any pending client requests.
            for (clientRequest <- phase1.pendingClientRequests) {
              processClientRequest(phase2, clientRequest)
            }
        }

      case phase212: Phase212 =>
        // Note that there can't be any pending requests to process.
        logger.checkEq(phase212.newPhase1.pendingClientRequests.size, 0)

        processPhase1b(phase212.newPhase1, phase1b) match {
          case None =>
            // Do nothing.
            {}

          case Some(values) =>
            val maxSlot = if (values.size == 0) {
              -1
            } else {
              values.lastKey
            }
            logger.checkLt(maxSlot, phase212.oldPhase2.nextSlot)

            // Propose values to the acceptors in entries [chosenWatermark,
            // maxSlot].
            for ((slot, value) <- values) {
              logger.check(!phase212.newPhase2.phase2bs.contains(slot))
              phase212.newPhase2.phase2bs(slot) = mutable.Map()
              val phase2a = Phase2a(slot = slot,
                                    round = phase212.newPhase2.round,
                                    value = value)
              for (index <- phase212.newPhase2.quorumSystem
                     .randomWriteQuorum()) {
                acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
              }
            }

            // Propose values to the acceptors in entries [maxSlot + 1,
            // oldPhase2.nextSlot). Typically, this range is subsumed by the
            // previous. maxSlot may be -1, if no votes were received. In this
            // case, we use chosenWatermark instead.
            for (slot <- Math.max(maxSlot + 1, chosenWatermark) until
                   phase212.oldPhase2.nextSlot) {
              logger.check(!phase212.newPhase2.phase2bs.contains(slot))
              phase212.newPhase2.phase2bs(slot) = mutable.Map()
              val phase2a = Phase2a(slot = slot,
                                    round = phase212.newPhase2.round,
                                    value = CommandOrNoop().withNoop(Noop()))
              for (index <- phase212.newPhase2.quorumSystem
                     .randomWriteQuorum()) {
                acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
              }
            }

            // At this point, we either transition to Phase22 or Phase2,
            // depending on whether or not the old Phase 2 has finished
            // choosing all of its values.
            if (chosenWatermark >= phase212.oldPhase2.nextSlot) {
              // Kick off GC by sending ExecutedWatermarkRequests to the
              // replicas.
              replicas.foreach(
                _.send(
                  ReplicaInbound()
                    .withExecutedWatermarkRequest(ExecutedWatermarkRequest())
                )
              )

              // Stop timers.
              stopTimers(phase212.oldPhase2)

              // Update our state.
              state = phase212.newPhase2.copy(
                gc = QueryingReplicas(
                  chosenWatermark = chosenWatermark,
                  maxSlot = maxSlot,
                  executedWatermarkReplies = mutable.Set(),
                  resendExecutedWatermarkRequests =
                    makeResendExecutedWatermarkRequestsTimer()
                )
              )
            } else {
              state = Phase22(
                oldPhase2 = phase212.oldPhase2,
                newPhase2 = phase212.newPhase2
              )
            }
        }
    }
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

      case matchmaking: Matchmaking =>
        // We'll process the client request after we've finished phase 1.
        matchmaking.pendingClientRequests += clientRequest

      case waitingForNewMatchmakers: WaitingForNewMatchmakers =>
        // We'll process the client request after we've finished phase 1.
        waitingForNewMatchmakers.pendingClientRequests += clientRequest

      case phase1: Phase1 =>
        // We'll process the client request after we've finished phase 1.
        phase1.pendingClientRequests += clientRequest

      case phase2: Phase2 =>
        processClientRequest(phase2, clientRequest)

      case phase2Matchmaking: Phase2Matchmaking =>
        processClientRequest(phase2Matchmaking.phase2, clientRequest)

      case phase212: Phase212 =>
        processClientRequest(phase212.newPhase2, clientRequest)

      case phase22: Phase22 =>
        processClientRequest(phase22.newPhase2, clientRequest)
    }
  }

  private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    state match {
      case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
          _: Phase1 =>
        logger.debug(
          s"Leader received a Phase2b message but is not in Phase2. Its " +
            s"state is $state. The Phase2b message is being ignored."
        )

      case phase2: Phase2 =>
        state = processPhase2b(phase2, phase2b)

      case phase2Matchmaking: Phase2Matchmaking =>
        state = phase2Matchmaking.copy(
          phase2 = processPhase2b(phase2Matchmaking.phase2, phase2b)
        )

      case phase212: Phase212 =>
        if (phase2b.round == phase212.oldPhase2.round) {
          state = phase212.copy(
            oldPhase2 = processPhase2b(phase212.oldPhase2, phase2b)
          )
        }
        if (phase2b.round == phase212.newPhase2.round) {
          state = phase212.copy(
            newPhase2 = processPhase2b(phase212.newPhase2, phase2b)
          )
        } else {
          logger.debug(
            s"A leader received a Phase2b message in round ${phase2b.round} " +
              s"but is in round ${phase212.oldPhase2.round}/" +
              s"${phase212.newPhase2.round}. The Phase2b is being " +
              s"ignored."
          )
          metrics.stalePhase2bsTotal.inc()
        }

      case phase22: Phase22 =>
        if (phase2b.round == phase22.oldPhase2.round) {
          state = phase22.copy(
            oldPhase2 = processPhase2b(phase22.oldPhase2, phase2b)
          )
        }
        if (phase2b.round == phase22.newPhase2.round) {
          state = phase22.copy(
            newPhase2 = processPhase2b(phase22.newPhase2, phase2b)
          )
        } else {
          logger.debug(
            s"A leader received a Phase2b message in round ${phase2b.round} " +
              s"but is in round ${phase22.oldPhase2.round}/" +
              s"${phase22.newPhase2.round}. The Phase2b is being " +
              s"ignored."
          )
          metrics.stalePhase2bsTotal.inc()
        }

        if (chosenWatermark >= phase22.oldPhase2.nextSlot) {
          // Kick off GC by sending ExecutedWatermarkRequests to the replicas.
          replicas.foreach(
            _.send(
              ReplicaInbound()
                .withExecutedWatermarkRequest(ExecutedWatermarkRequest())
            )
          )
          state = phase22.newPhase2.copy(
            gc = QueryingReplicas(
              // The numbers here are not perfect. chosenWatermark and maxSlot
              // might actually be a little lower. But, having them be larger
              // is not wrong.
              chosenWatermark = phase22.oldPhase2.nextSlot,
              maxSlot = phase22.oldPhase2.nextSlot,
              executedWatermarkReplies = mutable.Set(),
              resendExecutedWatermarkRequests =
                makeResendExecutedWatermarkRequestsTimer()
            )
          )
        }
    }
  }

  private def handleLeaderInfoRequest(
      src: Transport#Address,
      leaderInfoRequest: LeaderInfoRequest
  ): Unit = {
    state match {
      case _: Inactive =>
      // We're inactive, so we ignore the leader info request. The active
      // leader will respond to the request.

      case _: Matchmaking | _: WaitingForNewMatchmakers | _: Phase1 |
          _: Phase2 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound().withLeaderInfoReply(
            LeaderInfoReply(round = getRound(state))
          )
        )
    }
  }

  private def handleChosenWatermark(
      src: Transport#Address,
      msg: ChosenWatermark
  ): Unit = {
    state match {
      case _: Inactive =>
        chosenWatermark = Math.max(chosenWatermark, msg.watermark)

      case _: Matchmaking | _: WaitingForNewMatchmakers | _: Phase1 |
          _: Phase2 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        // We ignore these watermarks if we are trying to become an active
        // leader. Otherwise, things get too complicated. It's very unlikely
        // we'll ever get these messages anyway.
        logger.debug(
          s"Leader received a ChosenWatermark message but is active. Its " +
            s"state is $state. The message is being ignored."
        )
    }
  }

  private def handleMatchmakerNack(
      src: Transport#Address,
      nack: MatchmakerNack
  ): Unit = {
    if (nack.round < getRound(state)) {
      logger.debug(
        s"A Leader received a MatchmakerNack message with round " +
          s"${nack.round} but is already in round ${getRound(state)} in " +
          s"state $state. The Nack is being ignored."
      )
      metrics.staleMatchmakerNackTotal.inc()
      return
    }

    state match {
      case inactive: Inactive =>
        state = inactive.copy(round = nack.round)

      case _: WaitingForNewMatchmakers | _: Phase1 | _: Phase2 | _: Phase212 |
          _: Phase22 =>
        // Note that in Phase2, we might be performing GC with the matchmakers,
        // but GC requests do not produce nacks.
        logger.debug(
          s"Leader received a MatchmakerNack with round ${nack.round} but is " +
            s"not matchmaking. Its state is $state. The nack is being ignored."
        )

      case _: Matchmaking | _: Phase2Matchmaking =>
        // Note that if we're Phase2Matchmaking, we could maybe do a trick
        // where we increment the round of the Matchmaking phase and leave the
        // Phase 2 alone, but that's really complicated for not much gain.
        becomeLeader(
          roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
        )
    }
  }

  // TODO(mwhittaker): If we receive a nack, we perform a leader change. When
  // we perform this leader change, we lose all the pending commands. We might
  // want to leader change but put all the pending commands in the pending
  // list. This is not perfect though since the other leaders may also have
  // pending commands that we don't know about. Ultimately, we need good client
  // resend periods, which is not ideal, but it is what it is.
  private def handleAcceptorNack(
      src: Transport#Address,
      nack: AcceptorNack
  ): Unit = {
    val smallerRound =
      state match {
        case inactive: Inactive =>
          inactive.round
        case matchmaking: Matchmaking =>
          matchmaking.round
        case waiting: WaitingForNewMatchmakers =>
          waiting.round
        case phase1: Phase1 =>
          phase1.round
        case phase2: Phase2 =>
          phase2.round
        case phase2Matchmaking: Phase2Matchmaking =>
          phase2Matchmaking.phase2.round
        case phase212: Phase212 =>
          phase212.oldPhase2.round
        case phase22: Phase22 =>
          phase22.oldPhase2.round
      }

    if (nack.round < smallerRound) {
      logger.debug(
        s"A Leader received an AcceptorNack message with round " +
          s"${nack.round} but is already in round ${smallerRound}. " +
          s"The Nack is being ignored."
      )
      return
    }

    state match {
      case inactive: Inactive =>
        state = inactive.copy(round = nack.round)

      case _: Matchmaking | _: WaitingForNewMatchmakers =>
        logger.debug(
          s"Leader received an AcceptorNack with round ${nack.round} but is " +
            s"not in Phase 1 or 2. Its state is $state. The nack is being " +
            s"ignored."
        )

      case _: Phase1 | _: Phase2 | _: Phase2Matchmaking | _: Phase212 |
          _: Phase22 =>
        val newRound = roundSystem.nextClassicRound(
          leaderIndex = index,
          round = Math.max(nack.round, getRound(state))
        )
        becomeLeader(newRound)
    }
  }

  // This form of recovery is super heavy-handed. We really shouldn't be
  // running a complete leader change to recover a single value. However,
  // recovery should be extremeley rare, so we're ok.
  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    state match {
      case _: Inactive =>
        // Do nothing. The active leader will recover.
        {}

      case _: Matchmaking | _: WaitingForNewMatchmakers | _: Phase1 |
          _: Phase2 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        // If our chosen watermark is larger than the slot we're trying to
        // recover, then we won't get the value chosen again. We have to lower
        // our watermark so that the protocol gets the value chosen again.
        if (chosenWatermark > recover.slot) {
          chosenWatermark = recover.slot
        }

        // Leader change to make sure the slot is chosen.
        becomeLeader(getNextRound(state))
    }
  }

  private def handleExecutedWatermarkReply(
      src: Transport#Address,
      reply: ExecutedWatermarkReply
  ): Unit = {
    state match {
      // Note that we don't perform GC during an i/i+1 reconfiguration.
      case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
          _: Phase1 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        logger.debug(
          s"Leader received an ExecutedWatermarkReply but is not querying " +
            s"replicas. Its state is $state. The ExecutedWatermarkReply is " +
            s"being ignored."
        )

      case phase2: Phase2 =>
        phase2.gc match {
          case _: PushingToAcceptors | _: GarbageCollecting |
              _: WaitingForLargerChosenWatermark | Done | Cancelled =>
            logger.debug(
              s"Leader received an ExecutedWatermarkReply but is not " +
                s"querying replicas. Its state is $state. The " +
                s"ExecutedWatermarkReply is being ignored."
            )

          case gc: QueryingReplicas =>
            // Ignore lagging replies.
            if (reply.executedWatermark < gc.chosenWatermark) {
              return
            }

            // Wait until we have persisted on at least f+1 replicas.
            gc.executedWatermarkReplies.add(reply.replicaIndex)
            if (gc.executedWatermarkReplies.size < config.f + 1) {
              return
            }

            // Stop our timer.
            gc.resendExecutedWatermarkRequests.stop()

            // Notify the acceptors.
            val persisted = Persisted(persistedWatermark = gc.chosenWatermark)
            phase2.quorumSystem.nodes.foreach(
              acceptors(_).send(AcceptorInbound().withPersisted(persisted))
            )

            // Update our state.
            state = phase2.copy(
              gc = PushingToAcceptors(
                chosenWatermark = gc.chosenWatermark,
                maxSlot = gc.maxSlot,
                quorumSystem = phase2.quorumSystem,
                persistedAcks = mutable.Set(),
                resendPersisted =
                  makeResendPersistedTimer(persisted, phase2.quorumSystem.nodes)
              )
            )
        }
    }
  }

  private def handlePersistedAck(
      src: Transport#Address,
      reply: PersistedAck
  ): Unit = {
    // Note that we don't perform GC during an i/i+1 reconfiguration.
    state match {
      case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
          _: Phase1 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        logger.debug(
          s"Leader received an PersistedAck but is not persisting to " +
            s"acceptors. Its state is $state. The PersistedAck is " +
            s"being ignored."
        )

      case phase2: Phase2 =>
        phase2.gc match {
          case _: QueryingReplicas | _: GarbageCollecting |
              _: WaitingForLargerChosenWatermark | Done | Cancelled =>
            logger.debug(
              s"Leader received an PersistedAck but is not " +
                s"persisting to acceptors. Its state is $state. The " +
                s"PersistedAck is being ignored."
            )

          case gc: PushingToAcceptors =>
            // Ignore stale replies.
            if (reply.persistedWatermark < gc.chosenWatermark) {
              return
            }

            // Wait until we have received responses from a write quorum of
            // acceptors.
            gc.persistedAcks.add(reply.acceptorIndex)
            if (!gc.quorumSystem.isWriteQuorum(gc.persistedAcks.toSet)) {
              return
            }

            // Stop our timer.
            gc.resendPersisted.stop()

            // Wait until every command up to and including maxSlot has been
            // chosen.
            if (chosenWatermark <= gc.maxSlot) {
              state = phase2.copy(
                gc = WaitingForLargerChosenWatermark(
                  chosenWatermark = gc.chosenWatermark,
                  maxSlot = gc.maxSlot
                )
              )
              return
            }

            // Send garbage collect command.
            val garbageCollect = GarbageCollect(
              matchmakerConfiguration = matchmakerConfiguration,
              gcWatermark = phase2.round
            )
            matchmakerConfiguration.matchmakerIndex.foreach(
              matchmakers(_).send(
                MatchmakerInbound().withGarbageCollect(garbageCollect)
              )
            )

            // Update our state.
            state = phase2.copy(
              gc = GarbageCollecting(
                gcWatermark = phase2.round,
                matchmakerConfiguration = matchmakerConfiguration,
                garbageCollectAcks = mutable.Set(),
                resendGarbageCollects =
                  makeResendGarbageCollectsTimer(garbageCollect)
              )
            )
        }
    }
  }

  private def handleGarbageCollectAck(
      src: Transport#Address,
      garbageCollectAck: GarbageCollectAck
  ): Unit = {
    // Note that we don't perform GC during an i/i+1 reconfiguration.
    state match {
      case _: Inactive | _: Matchmaking | _: WaitingForNewMatchmakers |
          _: Phase1 | _: Phase2Matchmaking | _: Phase212 | _: Phase22 =>
        logger.debug(
          s"Leader received a GarbageCollectAck but is not garbage " +
            s"collecting. Its state is $state. The GarbageCollectAck is " +
            s"being ignored."
        )

      case phase2: Phase2 =>
        phase2.gc match {
          case _: QueryingReplicas | _: PushingToAcceptors |
              _: WaitingForLargerChosenWatermark | Done | Cancelled =>
            logger.debug(
              s"Leader received a GarbageCollectAck but is not " +
                s"garbage collecting. Its state is $state. The " +
                s"GarbageCollectAck is being ignored."
            )

          case gc: GarbageCollecting =>
            // Ignore stale replies.
            if (garbageCollectAck.epoch != gc.matchmakerConfiguration.epoch) {
              return
            }

            if (garbageCollectAck.gcWatermark < gc.gcWatermark) {
              return
            }

            // Wait until we have received responses from f+1 matchmakers.
            gc.garbageCollectAcks.add(garbageCollectAck.matchmakerIndex)
            if (gc.garbageCollectAcks.size < config.f + 1) {
              return
            }

            gc.resendGarbageCollects.stop()
            state = phase2.copy(gc = Done)
        }
    }
  }

  private def handleStopped(
      src: Transport#Address,
      stopped: Stopped
  ): Unit = {
    // Note that we don't perform GC during an i/i+1 reconfiguration.
    state match {
      case _: Inactive | _: WaitingForNewMatchmakers | _: Phase1 | _: Phase212 |
          _: Phase22 =>
        logger.debug(
          s"Leader received a Stopped but its state is $state. The Stopped " +
            s"is being ignored."
        )

      case phase2Matchmaking: Phase2Matchmaking =>
        // Give up and try again.
        becomeLeader(getNextRound(phase2Matchmaking))

      case matchmaking: Matchmaking =>
        // Ignore stale messages.
        if (stopped.epoch != matchmaking.matchmakerConfiguration.epoch) {
          return
        }
        matchmaking.resendMatchRequests.stop()

        val reconfigure = Reconfigure(
          matchmakerConfiguration = matchmaking.matchmakerConfiguration,
          newMatchmakerIndex = getRandomMatchmakers(config.numMatchmakers).toSeq
        )
        val reconfigurer = reconfigurers(rand.nextInt(reconfigurers.size))
        reconfigurer.send(ReconfigurerInbound().withReconfigure(reconfigure))

        state = WaitingForNewMatchmakers(
          round = matchmaking.round,
          matchmakerConfiguration = matchmaking.matchmakerConfiguration,
          quorumSystem = matchmaking.quorumSystem,
          quorumSystemProto = matchmaking.quorumSystemProto,
          pendingClientRequests = matchmaking.pendingClientRequests,
          resendReconfigure = makeResendReconfigureTimer(reconfigure)
        )

      case phase2: Phase2 =>
        phase2.gc match {
          case _: QueryingReplicas | _: PushingToAcceptors |
              _: WaitingForLargerChosenWatermark | Done | Cancelled =>
            logger.debug(
              s"Leader received a Stopped but its state is $state. The " +
                s"Stopped is being ignored."
            )

          case garbageCollecting: GarbageCollecting =>
            if (stopped.epoch !=
                  garbageCollecting.matchmakerConfiguration.epoch) {
              logger.debug(
                s"Leader received a Stopped but its state is $state. The " +
                  s"Stopped is being ignored."
              )
              return
            }
            garbageCollecting.resendGarbageCollects.stop()

            // We sent a GC command, but the current configuration is stopped.
            // We just give up for simplicity since the scenario is unlikely
            // and will get GCed by the future leader.
            state = phase2.copy(gc = Cancelled)
        }
    }

  }

  private def handleMatchChosen(
      src: Transport#Address,
      matchChosen: MatchChosen
  ): Unit = {
    if (matchChosen.value.epoch <= matchmakerConfiguration.epoch) {
      logger.debug(
        s"Leader received a MatchChosen in epoch ${matchChosen.value.epoch} " +
          s"but already has configuration $matchmakerConfiguration. The " +
          s"MatchChosen is being ignored."
      )
      return
    }

    matchmakerConfiguration = matchChosen.value

    state match {
      case _: Inactive | _: Phase1 | _: Phase2 | _: Phase2Matchmaking |
          _: Phase212 | _: Phase22 =>
        // Do nothing.
        ()

      case matchmaking: Matchmaking =>
        if (matchChosen.value.epoch <=
              matchmaking.matchmakerConfiguration.epoch) {
          return
        }
        matchmaking.resendMatchRequests.stop()
        state = startMatchmaking(matchmaking.round,
                                 matchmaking.pendingClientRequests,
                                 matchmaking.quorumSystem,
                                 matchmaking.quorumSystemProto)

      case waiting: WaitingForNewMatchmakers =>
        waiting.resendReconfigure.stop()
        state = startMatchmaking(waiting.round,
                                 waiting.pendingClientRequests,
                                 waiting.quorumSystem,
                                 waiting.quorumSystemProto)
    }
  }

  // API ///////////////////////////////////////////////////////////////////////
  // For the JS frontend.
  def reconfigure(): Unit = {
    becomeIIPlusOneLeader()
  }
}
