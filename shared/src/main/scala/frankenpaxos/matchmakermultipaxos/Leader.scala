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
    electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendMatchRequestsPeriod = java.time.Duration.ofSeconds(5),
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    resendPhase2asPeriod = java.time.Duration.ofSeconds(5),
    resendExecutedWatermarkRequestsPeriod = java.time.Duration.ofSeconds(5),
    resendPersistedPeriod = java.time.Duration.ofSeconds(5),
    resendGarbageCollectsPeriod = java.time.Duration.ofSeconds(5),
    sendChosenWatermarkEveryN = 100,
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

  val resendPeristedTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_leader_resend_persisted_total")
    .help("Total number of times the leader resent Persisted messages.")
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

  @JSExportAll
  case object Inactive extends State

  @JSExportAll
  case class Matchmaking(
      // The quorum system that this leader intends to use to get values chosen.
      quorumSystem: QuorumSystem[AcceptorIndex],
      // The MatchReplies received from the matchmakers.
      matchReplies: mutable.Map[MatchmakerIndex, MatchReply],
      // Any pending client requests. These client requests will be processed
      // as soon as the leader enters Phase 2.
      pendingClientRequests: mutable.Buffer[ClientRequest],
      // A timer to resend match requests, for liveness.
      resendMatchRequests: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase1(
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
      executedWatermarkReplies: mutable.Set[ReplicaIndex],
      resendExecutedWatermarkRequests: Transport#Timer
  ) extends GarbageCollection

  @JSExportAll
  case class PushingToAcceptors(
      persistedAcks: mutable.Set[AcceptorIndex],
      resendPersisted: Transport#Timer
  ) extends GarbageCollection

  @JSExportAll
  case object WaitingForLargerChosenWatermark extends GarbageCollection

  @JSExportAll
  case class GarbageCollecting(
      garbageCollectAcks: mutable.Set[MatchmakerIndex],
      resendGarbageCollects: Transport#Timer
  ) extends GarbageCollection

  @JSExportAll
  case object Done extends GarbageCollection

  @JSExportAll
  case class Phase2(
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
      chosenWatermark: Int,
      maxSlot: Int,
      gc: GarbageCollection
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

  // For simplicity, we use a round robin round system for the leaders.
  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // If the leader is the active leader, then this is its round. If it is
  // inactive, then this is the largest active round it knows about.
  @JSExport
  protected var round: Round = roundSystem
    .nextClassicRound(leaderIndex = 0, round = -1)

  // The next available slot in the log. Even though we have a next slot into
  // the log, you'll note that we don't even have a log.
  @JSExport
  protected var nextSlot: Slot = 0

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
      becomeLeader(
        roundSystem.nextClassicRound(leaderIndex = index, round = round)
      )
    } else {
      stopBeingLeader()
    }
  })

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    startMatchmaking(round = 0, pendingClientRequests = mutable.Buffer())
  } else {
    Inactive
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendMatchRequestsTimer(
      matchRequest: MatchRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendMatchRequests",
      options.resendMatchRequestsPeriod,
      () => {
        metrics.resendMatchRequestsTotal.inc()
        matchmakers.foreach(
          _.send(MatchmakerInbound().withMatchRequest(matchRequest))
        )
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

        state match {
          case Inactive | _: Matchmaking | _: Phase1 =>
            logger.fatal(
              s"The resendPhase2as timer was triggered but the leader is " +
                s"not in Phase2. Its state is $state."
            )

          case phase2: Phase2 =>
            phase2.values.get(chosenWatermark) match {
              case Some(value) =>
                val phase2a =
                  Phase2a(slot = chosenWatermark, round = round, value = value)
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

  private def startMatchmaking(
      round: Round,
      pendingClientRequests: mutable.Buffer[ClientRequest]
  ): Matchmaking = {
    // Send MatchRequests to the matchmakers.
    val (quorumSystem, quorumSystemProto) = getRandomQuorumSystem(
      config.numAcceptors
    )
    val matchRequest = MatchRequest(
      configuration =
        Configuration(round = round, quorumSystem = quorumSystemProto)
    )
    matchmakers.foreach(
      _.send(MatchmakerInbound().withMatchRequest(matchRequest))
    )

    // Return our state.
    Matchmaking(
      quorumSystem = quorumSystem,
      matchReplies = mutable.Map(),
      pendingClientRequests = pendingClientRequests,
      resendMatchRequests = makeResendMatchRequestsTimer(matchRequest)
    )
  }

  private def processClientRequest(
      phase2: Phase2,
      clientRequest: ClientRequest
  ): Unit = {
    // processClientRequest should only be called in Phase 2.
    logger.checkEq(state, phase2)

    // Send Phase2as to a write quorum.
    val slot = nextSlot
    nextSlot += 1
    val value = CommandOrNoop().withCommand(clientRequest.command)
    val phase2a = Phase2a(slot = slot, round = round, value = value)
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

    state match {
      case Inactive =>
        // Do nothing.
        {}
      case matchmaking: Matchmaking =>
        matchmaking.resendMatchRequests.stop()
        state = Inactive
      case phase1: Phase1 =>
        phase1.resendPhase1as.stop()
        state = Inactive
      case phase2: Phase2 =>
        phase2.resendPhase2as.stop()
        state = Inactive
    }
  }

  private def becomeLeader(newRound: Round): Unit = {
    logger.checkGt(newRound, round)
    logger.check(roundSystem.leader(newRound) == index)
    metrics.becomeLeaderTotal.inc()

    state match {
      case Inactive =>
        round = newRound
        state = startMatchmaking(round, mutable.Buffer())
      case matchmaking: Matchmaking =>
        matchmaking.resendMatchRequests.stop()
        round = newRound
        state = startMatchmaking(round, matchmaking.pendingClientRequests)
      case phase1: Phase1 =>
        phase1.resendPhase1as.stop()
        round = newRound
        state = startMatchmaking(round, phase1.pendingClientRequests)
      case phase2: Phase2 =>
        phase2.resendPhase2as.stop()
        round = newRound
        state = startMatchmaking(round, mutable.Buffer())
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
        case Request.Stopped(_)                => "Stopped"
        case Request.AcceptorNack(_)           => "AcceptorNack"
        case Request.Recover(_)                => "Recover"
        case Request.ExecutedWatermarkReply(_) => "ExecutedWatermarkReply"
        case Request.PersistedAck(_)           => "PersistedAck"
        case Request.GarbageCollectAck(_)      => "GarbageCollectAck"
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
        case Request.Stopped(r)           => handleStopped(src, r)
        case Request.AcceptorNack(r)      => handleAcceptorNack(src, r)
        case Request.Recover(r)           => handleRecover(src, r)
        case Request.ExecutedWatermarkReply(r) =>
          handleExecutedWatermarkReply(src, r)
        case Request.GarbageCollectAck(r) => handleGarbageCollectAck(src, r)
        case Request.PersistedAck(r)      => handlePersistedAck(src, r)
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
      case Inactive | _: Phase1 | _: Phase2 =>
        logger.debug(
          s"Leader received a MatchReply but is not currently in the " +
            s"Matchmaking phase (its state is $state). The MatchReply is " +
            s"being ignored."
        )

      case matchmaking: Matchmaking =>
        // TODO(mwhittaker): Rethink this once we implement matchmaker
        // reconfiguration.

        // Ignore stale rounds.
        if (matchReply.round != round) {
          logger.debug(
            s"Leader received a MatchReply in round ${matchReply.round} but " +
              s"is already in round $round. The MatchReply is being ignored."
          )
          metrics.staleMatchRepliesTotal.inc()
          // We can't receive MatchReplies from the future.
          logger.checkLt(matchReply.round, round)
          return
        }

        // Wait until we have a quorum of responses.
        matchmaking.matchReplies(matchReply.matchmakerIndex) = matchReply
        if (matchmaking.matchReplies.size < config.quorumSize) {
          return
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

        for {
          reply <- matchmaking.matchReplies.values
          configuration <- reply.configuration
        } {
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

        // If there are no pending rounds, then we're done already! We can skip
        // straight to phase 2. Otherwise, we have to go through phase 1.
        if (pendingRounds.isEmpty) {
          // In this case, we can issue GarbageCollect commands immediately
          // because we no values have been chosen in any log entry in any
          // round less than us. However, we didn't receive any configurations
          // anyway, so there is no need to garbage collect in the first place.
          nextSlot = chosenWatermark
          val phase2 = Phase2(
            quorumSystem = matchmaking.quorumSystem,
            values = mutable.Map(),
            phase2bs = mutable.Map(),
            chosen = mutable.Set(),
            numChosenSinceLastWatermarkSend = 0,
            resendPhase2as = makeResendPhase2asTimer(),
            chosenWatermark = chosenWatermark,
            maxSlot = -1,
            gc = Done
          )
          state = phase2

          // Process any pending client requests.
          for (clientRequest <- matchmaking.pendingClientRequests) {
            processClientRequest(phase2, clientRequest)
          }
        } else {
          // Send phase1s to acceptors.
          val phase1a =
            Phase1a(round = round, chosenWatermark = chosenWatermark)
          for (index <- acceptorIndices) {
            acceptors(index).send(AcceptorInbound().withPhase1A(phase1a))
          }

          // Update our state.
          state = Phase1(
            quorumSystem = matchmaking.quorumSystem,
            previousQuorumSystems = previousQuorumSystems.toMap,
            acceptorToRounds = acceptorToRounds.toMap,
            pendingRounds = pendingRounds,
            phase1bs = mutable.Map(),
            pendingClientRequests = matchmaking.pendingClientRequests,
            resendPhase1as =
              makeResendPhase1asTimer(phase1a, acceptorToRounds.keys.toSet)
          )
        }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    state match {
      case Inactive | _: Matchmaking | _: Phase2 =>
        logger.debug(
          s"Leader received a Phase1b message but is not in Phase1. Its " +
            s"state is $state. The Phase1b message is being ignored."
        )

      case phase1: Phase1 =>
        // Ignore messages from stale rounds.
        if (phase1b.round != round) {
          logger.debug(
            s"A leader received a Phase1b message in round ${phase1b.round} " +
              s"but is in round $round. The Phase1b is being ignored."
          )
          metrics.stalePhase1bsTotal.inc()
          // We can't receive phase 1bs from the future.
          logger.checkLt(phase1b.round, round)
          return
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
          return
        }

        // Stop our timers.
        phase1.resendPhase1as.stop()

        // Find the largest slot with a vote, or -1 if there are no votes.
        val slotInfos = phase1.phase1bs.values.flatMap(_.info)
        val maxSlot = if (slotInfos.isEmpty) {
          -1
        } else {
          slotInfos.map(_.slot).max
        }

        // Now, we iterate from chosenWatermark to maxSlot proposing safe
        // values to the acceptors to fill in the log.
        val values = mutable.Map[Slot, CommandOrNoop]()
        val phase2bs = mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]()
        for (slot <- chosenWatermark to maxSlot) {
          val value = safeValue(phase1.phase1bs.values, slot)
          values(slot) = value
          phase2bs(slot) = mutable.Map()
          val phase2a = Phase2a(slot = slot, round = round, value = value)
          for (index <- phase1.quorumSystem.randomWriteQuorum()) {
            acceptors(index).send(AcceptorInbound().withPhase2A(phase2a))
          }
        }

        // We've filled in every slot up to and including maxSlot, so the next
        // slot is maxSlot + 1 (or chosenWatermark if maxSlot + 1 is smaller).
        nextSlot = Math.max(chosenWatermark, maxSlot + 1)

        // Send ExecutedWatermarkRequests to the replicas.
        replicas.foreach(
          _.send(
            ReplicaInbound()
              .withExecutedWatermarkRequest(ExecutedWatermarkRequest())
          )
        )

        // Update our state.
        phase1.resendPhase1as.stop()
        val phase2 = Phase2(
          quorumSystem = phase1.quorumSystem,
          values = values,
          phase2bs = phase2bs,
          chosen = mutable.Set(),
          numChosenSinceLastWatermarkSend = 0,
          resendPhase2as = makeResendPhase2asTimer(),
          chosenWatermark = chosenWatermark,
          maxSlot = maxSlot,
          gc = QueryingReplicas(
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
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    state match {
      case Inactive =>
        // If we're not the active leader but receive a request from a client,
        // then we send back a NotLeader message to let them know that they've
        // contacted the wrong leader.
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(ClientInbound().withNotLeader(NotLeader()))

      case matchmaking: Matchmaking =>
        // We'll process the client request after we've finished phase 1.
        matchmaking.pendingClientRequests += clientRequest

      case phase1: Phase1 =>
        // We'll process the client request after we've finished phase 1.
        phase1.pendingClientRequests += clientRequest

      case phase2: Phase2 =>
        processClientRequest(phase2, clientRequest)
    }
  }

  private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    state match {
      case Inactive | _: Matchmaking | _: Phase1 =>
        logger.debug(
          s"Leader received a Phase2b message but is not in Phase2. Its " +
            s"state is $state. The Phase2b message is being ignored."
        )

      case phase2: Phase2 =>
        // Ignore messages from stale rounds.
        if (phase2b.round != round) {
          logger.debug(
            s"A leader received a Phase2b message in round ${phase2b.round} " +
              s"but is in round $round. The Phase2b is being ignored."
          )
          metrics.stalePhase2bsTotal.inc()
          // We can't receive phase 2bs from the future.
          logger.checkLt(phase2b.round, round)
          return
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
          return
        }

        // Wait until we have a write quorum.
        val phase2bs = phase2.phase2bs(phase2b.slot)
        phase2bs(phase2b.acceptorIndex) = phase2b
        if (!phase2.quorumSystem.isWriteQuorum(phase2bs.keys.toSet)) {
          return
        }

        // Inform the replicas that the value has been chosen.
        for (replica <- replicas) {
          replica.send(
            ReplicaInbound().withChosen(
              Chosen(slot = phase2b.slot, value = phase2.values(phase2b.slot))
            )
          )
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
        if (phase2.gc == WaitingForLargerChosenWatermark &&
            chosenWatermark > phase2.maxSlot) {
          val garbageCollect = GarbageCollect(gcWatermark = round)
          matchmakers.foreach(
            _.send(MatchmakerInbound().withGarbageCollect(garbageCollect))
          )

          state = phase2.copy(
            gc = GarbageCollecting(
              garbageCollectAcks = mutable.Set(),
              resendGarbageCollects =
                makeResendGarbageCollectsTimer(garbageCollect)
            ),
            numChosenSinceLastWatermarkSend = numChosenSinceLastWatermarkSend
          )
        } else {
          state = phase2.copy(
            numChosenSinceLastWatermarkSend = numChosenSinceLastWatermarkSend
          )
        }
    }
  }

  private def handleLeaderInfoRequest(
      src: Transport#Address,
      leaderInfoRequest: LeaderInfoRequest
  ): Unit = {
    state match {
      case Inactive =>
      // We're inactive, so we ignore the leader info request. The active
      // leader will respond to the request.

      case _: Matchmaking | _: Phase1 | _: Phase2 =>
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound().withLeaderInfoReply(LeaderInfoReply(round = round))
        )
    }
  }

  private def handleChosenWatermark(
      src: Transport#Address,
      msg: ChosenWatermark
  ): Unit = {
    state match {
      case Inactive =>
        chosenWatermark = Math.max(chosenWatermark, msg.watermark)

      case _: Matchmaking | _: Phase1 | _: Phase2 =>
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
    if (nack.round <= round) {
      logger.debug(
        s"A Leader received a MatchmakerNack message with round " +
          s"${nack.round} but is already in round $round. The Nack is being " +
          s"ignored."
      )
      metrics.staleMatchmakerNackTotal.inc()
      return
    }

    state match {
      case Inactive =>
        round = nack.round

      case _: Matchmaking =>
        val newRound =
          roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
        becomeLeader(newRound)

      case _: Phase1 | _: Phase2 =>
        logger.debug(
          s"Leader received a MatchmakerNack with round ${nack.round} but is " +
            s"not matchmaking. Its state is $state. The nack is being ignored."
        )
    }
  }

  private def handleStopped(
      src: Transport#Address,
      stopped: Stopped
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
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
    if (nack.round <= round) {
      logger.debug(
        s"A Leader received an AcceptorNack message with round " +
          s"${nack.round} but is already in round $round. The Nack is being " +
          s"ignored."
      )
      return
    }

    state match {
      case Inactive =>
        round = nack.round

      case _: Matchmaking =>
        logger.debug(
          s"Leader received an AcceptorNack with round ${nack.round} but is " +
            s"not in Phase 1 or 2. Its state is $state. The nack is being " +
            s"ignored."
        )

      case _: Phase1 | _: Phase2 =>
        val newRound =
          roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
        becomeLeader(newRound)
    }
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    state match {
      case Inactive =>
        // Do nothing. The active leader will recover.
        {}

      case _: Matchmaking | _: Phase1 | _: Phase2 =>
        // If our chosen watermark is larger than the slot we're trying to
        // recover, then we won't get the value chosen again. We have to lower
        // our watermark so that the protocol gets the value chosen again.
        if (chosenWatermark > recover.slot) {
          chosenWatermark = recover.slot
        }

        // Leader change to make sure the slot is chosen.
        val newRound =
          roundSystem.nextClassicRound(leaderIndex = index, round = round)
        becomeLeader(newRound)
    }
  }

  private def handleExecutedWatermarkReply(
      src: Transport#Address,
      executedWatermarkReply: ExecutedWatermarkReply
  ): Unit = {
    state match {
      case Inactive | _: Matchmaking | _: Phase1 =>
        logger.debug(
          s"Leader received an ExecutedWatermarkReply but is not querying " +
            s"replicas. Its state is $state. The ExecutedWatermarkReply is " +
            s"being ignored."
        )

      case phase2: Phase2 =>
        phase2.gc match {
          case _: PushingToAcceptors | _: GarbageCollecting |
              WaitingForLargerChosenWatermark | Done =>
            logger.debug(
              s"Leader received an ExecutedWatermarkReply but is not " +
                s"querying replicas. Its state is $state. The " +
                s"ExecutedWatermarkReply is being ignored."
            )

          case gc: QueryingReplicas =>
            // Ignore lagging replies.
            if (executedWatermarkReply.executedWatermark < chosenWatermark) {
              return
            }

            // Wait until we have persisted on at least f+1 replicas.
            gc.executedWatermarkReplies.add(executedWatermarkReply.replicaIndex)
            if (gc.executedWatermarkReplies.size < config.f + 1) {
              return
            }

            // Stop our timer.
            gc.resendExecutedWatermarkRequests.stop()

            // Notify the acceptors.
            val persisted = Persisted(
              persistedWatermark = phase2.chosenWatermark
            )
            phase2.quorumSystem.nodes.foreach(
              acceptors(_).send(AcceptorInbound().withPersisted(persisted))
            )

            // Update our state.
            state = phase2.copy(
              gc = PushingToAcceptors(
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
      persistedAck: PersistedAck
  ): Unit = {
    state match {
      case Inactive | _: Matchmaking | _: Phase1 =>
        logger.debug(
          s"Leader received an PersistedAck but is not persisting to " +
            s"acceptors. Its state is $state. The PersistedAck is " +
            s"being ignored."
        )

      case phase2: Phase2 =>
        phase2.gc match {
          case _: QueryingReplicas | _: GarbageCollecting |
              WaitingForLargerChosenWatermark | Done =>
            logger.debug(
              s"Leader received an PersistedAck but is not " +
                s"persisting to acceptors. Its state is $state. The " +
                s"PersistedAck is being ignored."
            )

          case gc: PushingToAcceptors =>
            // Ignore stale replies.
            if (persistedAck.persistedWatermark < phase2.chosenWatermark) {
              return
            }

            // Wait until we have received responses from a write quorum of
            // acceptors.
            gc.persistedAcks.add(persistedAck.acceptorIndex)
            if (!phase2.quorumSystem.isWriteQuorum(gc.persistedAcks.toSet)) {
              return
            }

            // Stop our timer.
            gc.resendPersisted.stop()

            // Wait until every command up to and including maxSlot has been
            // chosen.
            if (chosenWatermark <= phase2.maxSlot) {
              state = phase2.copy(gc = WaitingForLargerChosenWatermark)
              return
            }

            // Send garbage collect command.
            val garbageCollect = GarbageCollect(gcWatermark = round)
            matchmakers.foreach(
              _.send(MatchmakerInbound().withGarbageCollect(garbageCollect))
            )

            // Update our state.
            state = phase2.copy(
              gc = GarbageCollecting(
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
    state match {
      case Inactive | _: Matchmaking | _: Phase1 =>
        logger.debug(
          s"Leader received a GarbageCollectAck but is not garbage " +
            s"collecting. Its state is $state. The GarbageCollectAck is " +
            s"being ignored."
        )

      case phase2: Phase2 =>
        phase2.gc match {
          case _: QueryingReplicas | _: PushingToAcceptors |
              WaitingForLargerChosenWatermark | Done =>
            logger.debug(
              s"Leader received a GarbageCollectAck but is not " +
                s"garbage collecting. Its state is $state. The " +
                s"GarbageCollectAck is being ignored."
            )

          case gc: GarbageCollecting =>
            // Ignore stale replies.
            if (garbageCollectAck.gcWatermark < round) {
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
}
