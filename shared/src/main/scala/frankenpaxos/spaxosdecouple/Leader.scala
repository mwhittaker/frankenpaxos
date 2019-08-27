package frankenpaxos.spaxosdecouple

import com.google.protobuf.ByteString
import frankenpaxos.{Actor, Logger, ProtoSerializer, Util}
import frankenpaxos.monitoring._
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.thrifty.ThriftySystem

import scala.collection.immutable.SortedMap
import scala.collection.{breakOut, mutable}
import scala.scalajs.js.annotation._

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = ReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class LeaderOptions(
                           thriftySystem: ThriftySystem,
                           resendPhase1asTimerPeriod: java.time.Duration,
                           resendPhase2asTimerPeriod: java.time.Duration,
                           // The leader buffers phase 2a messages. This buffer can grow to at most
                           // size `phase2aMaxBufferSize`. If the buffer does not fill up, then it is
                           // flushed after `phase2aBufferFlushPeriod`.
                           phase2aMaxBufferSize: Int,
                           phase2aBufferFlushPeriod: java.time.Duration,
                           // As with phase 2a messages, leaders buffer value chosen messages.
                           valueChosenMaxBufferSize: Int,
                           valueChosenBufferFlushPeriod: java.time.Duration,
                         )

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    thriftySystem = ThriftySystem.NotThrifty,
    resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(1e7.toLong),
    resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(1e7.toLong),
    phase2aMaxBufferSize = 25,
    phase2aBufferFlushPeriod = java.time.Duration.ofMillis(100),
    valueChosenMaxBufferSize = 100,
    valueChosenBufferFlushPeriod = java.time.Duration.ofSeconds(5)
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val executeGraphTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_execute_graph_total")
    .help("Total number of times the replica executed the dependency graph.")
    .register()

  val executeGraphTimerTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_execute_graph_timer_total")
    .help(
      "Total number of times the replica executed the dependency graph from " +
        "a timer."
    )
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val committedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_committed_commands_total")
    .help(
      "Total number of commands that were committed (with potential " +
        "duplicates)."
    )
    .register()

  val preparePhasesStartedTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_prepare_phases_started_total")
    .labelNames("type") // "fast" or "classic".
    .help("Total number of prepare phases started.")
    .register()

  val resendPreAcceptsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_resend_pre_accepts_total")
    .help("Total number of times the leader resent PreAccept messages.")
    .register()

  val resendAcceptsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_resend_accepts_total")
    .help("Total number of times the leader resent Accept messages.")
    .register()

  val resendPreparesTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_resend_prepares_total")
    .help("Total number of times the leader resent Prepare messages.")
    .register()

  val defaultToSlowPathTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_default_to_slow_path_total")
    .help("Total number of times the leader defaulted to the slow path.")
    .register()

  val dependencyGraphNumVertices: Gauge = collectors.gauge
    .build()
    .name("epaxos_replica_dependency_graph_num_vertices")
    .help("The number of vertices in the dependency graph.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("epaxos_replica_dependencies")
    .help("The number of dependencies that a command has.")
    .register()

  val chosenCommandsTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_chosen_commands_total")
    .labelNames("type") // "fast" or "classic".
    .help(
    "Total number of commands that were chosen (with potential duplicates)."
  )
    .register()

  val leaderChangesTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_leader_changes_total")
    .help("Total number of leader changes.")
    .register()

  val stuckTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_stuck_total")
    .help("Total number of times the leader got stuck in phase 2.")
    .register()

  val chosenWatermark: Gauge = collectors.gauge
    .build()
    .name("fast_multipaxos_leader_chosen_watermark")
    .help("The index at which all smaller log entries have been chosen.")
    .register()

  val nextSlot: Gauge = collectors.gauge
    .build()
    .name("fast_multipaxos_leader_next_slot")
    .help("The next free slot in the log.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_resend_phase1as_total")
    .help("Total number of times the leader resent phase 1a messages.")
    .register()

  val resendPhase2asTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_resend_phase2as_total")
    .help("Total number of times the leader resent phase 2a messages.")
    .register()

  val phase2aBufferFullTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_phase2a_buffer_full_total")
    .help("Total number of times the phase 2a buffer filled up.")
    .register()

  val phase2aBufferFlushTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_phase2a_buffer_flush_total")
    .help("Total number of times the phase 2a buffer was flushed by a timer.")
    .register()

  val valueChosenBufferFullTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_value_chosen_buffer_full_total")
    .help("Total number of times the value chosen buffer filled up.")
    .register()

  val valueChosenBufferFlushTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_value_chosen_buffer_flush_total")
    .help(
      "Total number of times the value chosen buffer was flushed by a timer."
    )
    .register()
}

// Note that there are a couple of types (e.g., CmdLogEntry, LeaderState) that
// you might expect to be in the Replica class instead of the Replica
// companion. Unfortunately, there is a technical reason related to scala.js
// that prevents us from doing so.
//
// If we place CmdLogEntry or LeaderState within the Replica class, then it
// becomes an inner class. When scala.js compiles an instance of an inner
// class, it includes a pointer to the enclosing object. For example, an
// instance of NoCommandEntry would end up looking something like this:
//
//   NoCommandEntry(
//       replica = ...,        // Something of type Replica.
//       ballot = Ballot(...),
//   )
//
// I don't know why scala.js does this. I don't even understand how it does
// this. Shouldn't you be able to instantiate a NoCommandEntry without an
// enclosing replica? Who knows.
//
// Anyway, when an object like this is put within a map within a Replica
// instance, and that map is displayed using <frankenpaxos-map>, then Vue does
// a deep watch on the map. The deep watch on the map watches the objects
// within the map, and since those objects have pointers to a Replica, Vue ends
// up watching every field inside Replica. This is especially bad because a
// Replica object includes a dependency graph which is cyclic. When Vue
// encounters cyclic data structures, it can overflow the stack and crash.
//
// To avoid these issues, we pull types out of the Replica class and put them
// into the companion Replica object. This prevents scala.js from embedding a
// parent pointer to a Replica, which prevents Vue from watching all fields
// within Replica, which prevents Vue from crashing. It's annoying and very
// subtle, but it's the best we can do for now.
@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer

  type ReplicaIndex = Int
  type ClientPseudonym = Int
  type ClientId = Int

}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
                                                               address: Transport#Address,
                                                               transport: Transport,
                                                               logger: Logger,
                                                               config: Config[Transport],
                                                               // Public for the JS visualizations.
                                                               stateMachine: StateMachine,
                                                               options: LeaderOptions = LeaderOptions.default,
                                                               metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
                                                             ) extends Actor(address, transport, logger) {
  import Replica._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer


  // Fields ////////////////////////////////////////////////////////////////////
  logger.check(config.valid())
  logger.check(config.replicaAddresses.contains(address))
  private val index: ReplicaIndex = config.replicaAddresses.indexOf(address)

  private val me: Chan[Replica[Transport]] =
    chan[Replica[Transport]](address, Replica.serializer)

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (replicaAddress <- config.replicaAddresses)
      yield chan[Replica[Transport]](replicaAddress, Replica.serializer)

  private val otherReplicaAddresses: Seq[Transport#Address] =
    config.replicaAddresses.filter(_ != address)

  private val otherReplicas: Seq[Chan[Replica[Transport]]] =
    for (a <- otherReplicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

  private val otherReplicasByAddress
  : Map[Transport#Address, Chan[Replica[Transport]]] = {
    for (a <- otherReplicaAddresses)
      yield a -> chan[Replica[Transport]](a, Replica.serializer)
  }.toMap

  type AcceptorId = Int
  type Round = Int
  type Slot = Int
  type ClientPseudonym = Int
  type ClientId = Int

  @JSExportAll
  sealed trait Entry
  case class ECommand(uniqueId: UniqueId) extends Entry
  object ENoop extends Entry

  sealed trait LeaderState[Transport <: frankenpaxos.Transport[Transport]]

  @JSExport
  protected var round: Round = 0

  // The log of chosen commands. Public for testing.
  @JSExportAll
  val log: mutable.SortedMap[Slot, Entry] = mutable.SortedMap()

  // The client table records the response to the latest request from each
  // client. For example, if command c1 sends command x with id 2 to a leader
  // and the leader later executes x yielding response y, then c1 maps to (2,
  // y) in the client table.
  //
  // TODO(mwhittaker): Extract out a client table abstraction?
  @JSExport
  protected var clientTable =
  mutable.Map[(Transport#Address, ClientPseudonym), (ClientId, Array[Byte])]()

  // At any point in time, the leader knows that all slots less than
  // chosenWatermark have been chosen. That is, for every `slot` <
  // chosenWatermark, there is an Entry for `slot` in `log`.
  @JSExport
  protected var chosenWatermark: Slot = 0

  // The next slot in which to propose a command.
  //
  // TODO(mwhittaker): Add a buffer to prevent the leader from running too far
  // ahead.
  @JSExport
  protected var nextSlot: Slot = 0

  // The state of the leader.
  @JSExportAll
  sealed trait State

  // This leader is not the active leader.
  @JSExportAll
  case object Inactive extends State

  // This leader is executing phase 1.
  @JSExportAll
  case class Phase1(
                     // Phase 1b responses.
                     phase1bs: mutable.Map[AcceptorId, Phase1b],
                     // Pending proposals. When a leader receives a proposal during phase 1,
                     // it buffers the proposal and replays it once it enters phase 2.
                     pendingProposals: mutable.Buffer[UniqueId],
                     // A timer to resend phase 1as.
                     resendPhase1as: Transport#Timer
                   ) extends State

  private val resendPhase1asTimer: Transport#Timer = timer(
    "resendPhase1as",
    options.resendPhase1asTimerPeriod,
    () => {
      sendPhase1as(thrifty = false)
      resendPhase1asTimer.start()
      metrics.resendPhase1asTotal.inc()
    }
  )

  // This leader has finished executing phase 1 and is now executing phase 2.
  @JSExportAll
  case class Phase2(
                     // In a classic round, leaders receive commands from clients and relay
                     // them on to acceptors. pendingEntries stores these commands that are
                     // pending votes. Note that during a fast round, a leader may not have a
                     // pending command for a slot, even though it does have phase 2bs for it.
                     pendingEntries: mutable.SortedMap[Slot, Entry],
                     // For each slot, the set of phase 2b messages for that slot. In a
                     // classic round, all the phase 2b messages will be for the same command.
                     // In a fast round, they do not necessarily have to be.
                     phase2bs: mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]],
                     // A timer to resend all pending phase 2a messages.
                     resendPhase2as: Transport#Timer,
                     // A set of buffered phase 2a messages.
                     phase2aBuffer: mutable.Buffer[Phase2a],
                     // A timer to flush the buffer of phase 2a messages.
                     phase2aBufferFlushTimer: Transport#Timer,
                     // A set of buffered value chosen messages.
                     valueChosenBuffer: mutable.Buffer[ValueChosen],
                     // A timer to flush the buffer of value chosen messages.
                     valueChosenBufferFlushTimer: Transport#Timer
                   ) extends State


  // Public for testing.

  private val resendPhase2asTimer: Transport#Timer = timer(
    "resendPhase2as",
    options.resendPhase2asTimerPeriod,
    () => {
      resendPhase2as()
      resendPhase2asTimer.start()
      metrics.resendPhase2asTotal.inc()
    }
  )

  private val phase2aBufferFlushTimer: Transport#Timer = timer(
    "phase2aBufferFlush",
    options.phase2aBufferFlushPeriod,
    () => {
      flushPhase2aBuffer(thrifty = true)
      metrics.phase2aBufferFlushTotal.inc()
    }
  )

  private val valueChosenBufferFlushTimer: Transport#Timer = timer(
    "valueChosenBufferFlush",
    options.valueChosenBufferFlushPeriod,
    () => {
      flushValueChosenBuffer()
      metrics.valueChosenBufferFlushTotal.inc()
    }
  )

  @JSExport
  protected var state: State =
    if (round == 0) {
      sendPhase1as(thrifty = true)
      resendPhase1asTimer.start()
      Phase1(mutable.Map(), mutable.Buffer(), resendPhase1asTimer)
    } else {
      Inactive
    }

  // Custom logger.
  val leaderLogger = new Logger(frankenpaxos.LogDebug) {
    private def withInfo(s: String): String = {
      val stateString = state match {
        case Phase1(_, _, _)             => "Phase 1"
        case Phase2(_, _, _, _, _, _, _) => "Phase 2"
        case Inactive                    => "Inactive"
      }
      s"[$stateString, round=$round] " + s
    }

    override def fatalImpl(message: String): Nothing =
      logger.fatal(withInfo(message))

    override def errorImpl(message: String): Unit =
      logger.error(withInfo(message))

    override def warnImpl(message: String): Unit =
      logger.warn(withInfo(message))

    override def infoImpl(message: String): Unit =
      logger.info(withInfo(message))

    override def debugImpl(message: String): Unit =
      logger.debug(withInfo(message))

  }

  @JSExport
  protected val requests: mutable.Set[ClientRequest] = mutable.Set()

  @JSExport
  protected val acks: mutable.Map[UniqueId, Int] = mutable.Map()

  @JSExport
  protected val stableIds: mutable.Set[UniqueId] = mutable.Set()

  @JSExport
  protected val leaderIndex: Int = 0

  @JSExport
  protected val proposed: mutable.Set[UniqueId] = mutable.Set()

  def handleDecide(src: Transport#Address, r: Decide): Unit = ???

  def handlePhase1a(src: Transport#Address, r: Phase1a): Unit = ???

  def handlePhase2aBuffer(src: Transport#Address, r: Phase2aBuffer): Unit = ???

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
                        src: Transport#Address,
                        inbound: ReplicaInbound
                      ): Unit = {
    inbound.request match {
      case Request.ClientRequest(r) => handleClientRequest(src, r)
      case Request.Acknowledge(r) => handleAcknowledge(src, r)
      case Request.Decide(r) => handleDecide(src, r)
      case Request.Forward(r) => handleForward(src, r)
      case Request.Phase1A(r) => handlePhase1a(src, r)
      case Request.Phase1BNack(r) => handlePhase1bNack(src, r)
      case Request.Phase2ABuffer(r) => handlePhase2aBuffer(src, r)
      case Request.Phase2BBuffer(r) => handlePhase2bBuffer(src, r)
      case Request.ValueChosenBuffer(r) => handleValueChosenBuffer(src, r)
      case Request.Proposal(r)          => handleProposal(src, r)
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
                                   src: Transport#Address,
                                   request: ClientRequest
                                 ): Unit = {
    val client = chan[Client[Transport]](src, Client.serializer)

    // TODO(mwhittaker): Ignore requests that are older than the current id
    // stored in the client table.

    // If we've cached the result of this proposed command in the client table,
    // then we can reply to the client directly. Note that only the leader
    // replies to the client since ProposeReplies include the round of the
    // leader, and only the leader knows this.
    clientTable.get((src, request.uniqueId.clientPseudonym)) match {
      case Some((clientId, result)) =>
        if (request.uniqueId.clientId == clientId && state != Inactive) {
          leaderLogger.debug(
            s"The latest command for client $src pseudonym " +
              s"${request.uniqueId.clientPseudonym} (i.e., command $clientId)" +
              s"was found in the client table."
          )
          client.send(
            ClientInbound().withClientReply(
              ClientReply(clientPseudonym = request.uniqueId.clientPseudonym,
                clientId = clientId,
                result = ByteString.copyFrom(result))
            )
          )
          return
        }
      case None =>
    }

    for (replica <- replicas) {
      replica.send(ReplicaInbound().withForward(Forward(request)))
    }
  }

  private def handleForward(
                             src: Transport#Address,
                             forward: Forward
                           ): Unit = {
    metrics.requestsTotal.labels("Forward").inc()

    val replica = chan[Replica[Transport]](src, Replica.serializer)
    requests.add(forward.clientRequest)
    replica.send(ReplicaInbound().withAcknowledge(Acknowledge(forward.clientRequest.uniqueId)))
  }

  def handleProposal(src: Transport#Address, proposal: Proposal) = {
    state match {
      case Inactive =>
        leaderLogger.debug(
          s"Leader received propose request but is inactive."
        )

      case Phase1(_, pendingProposals, _) =>
        // We buffer all pending proposals in phase 1 and process them later
        // when we enter phase 2.
        pendingProposals += uniqueId

      case Phase2(pendingEntries,
      phase2bs,
      _,
      phase2aBuffer,
      phase2aBufferFlushTimer,
      _,
      _) =>

        phase2aBuffer += Phase2a(slot = nextSlot, round = round).withUniqueId(uniqueId)
        pendingEntries(nextSlot) = ECommand(uniqueId)
        phase2bs(nextSlot) = mutable.Map()
        nextSlot += 1
        if (phase2aBuffer.size >= options.phase2aMaxBufferSize) {
          metrics.phase2aBufferFullTotal.inc()
          flushPhase2aBuffer(thrifty = true)
        }
    }
  }

  private def handleAcknowledge(
                                 src: Transport#Address,
                                 acknowledge: Acknowledge
                               ): Unit = {
    metrics.requestsTotal.labels("Acknowledge").inc()

    acks.put(acknowledge.uniqueId, acks.getOrElse(acknowledge.uniqueId, 0) + 1)

    val f: Int = 1
    if (acks.getOrElse(acknowledge.uniqueId, 0) >= f + 1) {
      stableIds.add(acknowledge.uniqueId)
      // This replica is the leader
      if (leaderIndex == index && !proposed.contains(acknowledge.uniqueId)) {
        proposed.add(acknowledge.uniqueId)
        handleProposal(acknowledge.uniqueId)
      }
    }
  }

  // In Fast Paxos, every acceptor has a single vote round and vote value. With
  // Fast MultiPaxos, we have one pair of vote round and vote value per slot.
  // We call such a pair a vote. The vote also includes the highest round in
  // which an acceptor has received a distinguished "any" value.
  @JSExportAll
  sealed trait VoteValue
  case class VVCommand(command: Command) extends VoteValue
  case object VVNoop extends VoteValue
  case object VVNothing extends VoteValue

  @JSExportAll
  case class Entry(
                    voteRound: Int,
                    voteValue: VoteValue,
                    anyRound: Option[Int]
                  )

  // The log of votes.
  @JSExport
  protected val log: Log[Entry] = new Log()

  // If this acceptor receives a propose request from a client, it attempts to
  // choose the command in `nextSlot`.
  @JSExport
  protected var nextSlot = 0;


  private def handlePhase1a(src: Transport#Address, phase1a: Phase1a): Unit = {
    metrics.requestsTotal.labels("Phase1a").inc()

    // Ignore messages from previous rounds.
    if (phase1a.round <= round) {
      logger.info(
        s"An acceptor received a phase 1a message for round " +
          s"${phase1a.round} but is in round $round."
      )
      val leader = chan[Replica[Transport]](src, Replica.serializer)
      leader.send(
        ReplicaInbound().withPhase1BNack(
          Phase1bNack(replicaId = config.replicaAddresses.indexOf(me), round = round)
        )
      )
      return
    }

    // Bump our round and send the leader all of our votes. Note that we
    // exclude votes below the chosen watermark and votes for slots that the
    // leader knows are chosen. We also make sure not to return votes for slots
    // that we haven't actually voted in.
    round = phase1a.round
    val votes = log
      .prefix()
      .iteratorFrom(phase1a.chosenWatermark)
      .filter({ case (slot, _) => !phase1a.chosenSlot.contains(slot) })
      .flatMap({
        case (s, Entry(vr, VVCommand(command), _)) =>
          Some(Phase1bVote(slot = s, voteRound = vr).withUniqueId(UniqueId(command.clientAddress, command.clientPseudonym, command.clientId)))
        case (s, Entry(vr, VVNoop, _)) =>
          Some(Phase1bVote(slot = s, voteRound = vr).withNoop(Noop()))
        case (_, Entry(_, VVNothing, _)) =>
          None
      })
    val leader = replicas(leaderIndex)
    leader.send(
      ReplicaInbound().withPhase1B(
        Phase1b(replicaId = config.replicaAddresses.indexOf(me), round = round, vote = votes.to[Seq])
      )
    )
  }


  private def handlePhase1b(
                             src: Transport#Address,
                             request: Phase1b
                           ): Unit = {
    metrics.requestsTotal.labels("Phase1b").inc()
    state match {
      case Inactive =>
        leaderLogger.debug(
          s"Leader received phase 1b from $src, but is inactive."
        )

      case Phase2(_, _, _, _, _, _, _) =>
        leaderLogger.debug(
          s"Leader received phase 1b from $src, but is in phase 2b in " +
            s"round $round."
        )

      case Phase1(phase1bs, pendingProposals, resendPhase1as) =>
        if (request.round != round) {
          leaderLogger.debug(
            s"Leader received phase 1b from $src in round ${request.round}, " +
              s"but is in round $round."
          )
          return
        }

        // Wait until we receive a quorum of phase 1bs.
        phase1bs(request.replicaId) = request
        if (phase1bs.size < config.f + 1) {
          return
        }

        // If we do have a quorum of phase 1bs, then we transition to phase 2.
        resendPhase1as.stop()

        // `phase1bs` maps each acceptor to a list of phase1b votes. We index
        // each of these lists by slot.
        type VotesBySlot = SortedMap[Slot, Phase1bVote]
        val votes: collection.Map[AcceptorId, VotesBySlot] =
          phase1bs.mapValues((phase1b) => {
            phase1b.vote.map(vote => vote.slot -> vote)(breakOut): VotesBySlot
          })

        // The leader's log contains chosen entries for some slots, and the
        // acceptors have voted for some slots. This looks something like this:
        //
        //                                     chosenWatermark
        //                                    /                   endSlot
        //                                   /                   /
        //                      0   1   2   3   4   5   6   7   8   9
        //                    +---+---+---+---+---+---+---+---+---+---+
        //               log: | x | x | x |   |   | x |   |   | x |   |
        //                    +---+---+---+---+---+---+---+---+---+---+
        //   acceptor 0 vote:               x   x
        //   acceptor 1 vote:                   x       x
        //   acceptor 2 vote:               x       x   x
        //
        // The leader does not want gaps in the log, so it attempts to choose
        // as many slots as possible to remove the gaps. In the example above,
        // the leader would propose in slots 3, 4, 6, and 7. Letting endSlot =
        // 8, these are the unchosen slots in the range [chosenWatermark,
        // endSlot].
        //
        // In the example above, endSlot is 8 because it is the largest chosen
        // slot. However, in the example below, it is 9 because an acceptor has
        // voted in slot 9. Thus, we let endSlot be the larger of (a) the
        // largest chosen slot and (b) the largest slot with a phase1b vote.
        //
        //                                     chosenWatermark
        //                                    /                       endSlot
        //                                   /                       /
        //                      0   1   2   3   4   5   6   7   8   9
        //                    +---+---+---+---+---+---+---+---+---+---+
        //               log: | x | x | x |   |   | x |   |   | x |   |
        //                    +---+---+---+---+---+---+---+---+---+---+
        //   acceptor 0 vote:               x   x                   x
        //   acceptor 1 vote:                   x       x   x
        //   acceptor 2 vote:               x       x   x           x
        val endSlot: Int = math.max(
          votes
            .map({ case (a, vs) => if (vs.size == 0) -1 else vs.lastKey })
            .max,
          if (log.size == 0) -1 else log.lastKey
        )

        val pendingEntries = mutable.SortedMap[Slot, Entry]()
        val phase2bs =
          mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]]()
        val phase2aBuffer = mutable.Buffer[Phase2a]()

        // For every unchosen slot between the chosenWatermark and endSlot,
        // choose a value to propose and propose it. We also collect a set of
        // other commands to propose and propose them later.
        val proposedCommands = mutable.Set[UniqueId]()
        val yetToProposeCommands = mutable.Set[UniqueId]()
        for (slot <- chosenWatermark to endSlot) {
          val (proposal, commands) = chooseProposal(votes, slot)
          yetToProposeCommands ++= commands

          val phase2a = proposal match {
            case ECommand(command) =>
              proposedCommands += command
              Phase2a(slot = slot, round = round).withUniqueId(command)
            case ENoop =>
              Phase2a(slot = slot, round = round).withNoop(Noop())
          }
          phase2aBuffer += phase2a
          pendingEntries(slot) = proposal
          phase2bs(slot) = mutable.Map[AcceptorId, Phase2b]()
        }

        state = Phase2(pendingEntries,
          phase2bs,
          resendPhase2asTimer,
          phase2aBuffer,
          phase2aBufferFlushTimer,
          mutable.Buffer[ValueChosen](),
          valueChosenBufferFlushTimer)
        resendPhase2asTimer.start()
        phase2aBufferFlushTimer.start()
        valueChosenBufferFlushTimer.start()

        // Replay the pending proposals.
        nextSlot = endSlot + 1
        for (proposal <- pendingProposals) {
          phase2aBuffer += Phase2a(slot = nextSlot, round = round)
            .withUniqueId(proposal)
          phase2bs(nextSlot) = mutable.Map()
          nextSlot += 1
        }

        // Replay the safe to propose values that we haven't just proposed.
        for (command <- yetToProposeCommands.diff(proposedCommands)) {
          phase2aBuffer += Phase2a(slot = nextSlot, round = round)
            .withUniqueId(command)
          pendingEntries(nextSlot) = ECommand(command)
          phase2bs(nextSlot) = mutable.Map()
          nextSlot += 1
        }

        flushPhase2aBuffer(thrifty = true)
    }
  }

  private def handlePhase1bNack(
                                 src: Transport#Address,
                                 nack: Phase1bNack
                               ): Unit = {
    metrics.requestsTotal.labels("Phase1bNack").inc()
    state match {
      case Inactive | Phase2(_, _, _, _, _, _, _) =>
        leaderLogger.debug(
          s"Leader received phase 1b nack from $src, but is not in phase 1."
        )

      case Phase1(_, _, _) =>
        if (nack.round > round) {
          leaderLogger.debug(
            s"Leader running phase 1 in round $round got nack in round " +
              s"${nack.round} from $src. Increasing round."
          )
          // TODO(mwhittaker): Check that the nack is not out of date.
          // TODO(mwhittaker): Change to a round that we own!
        }
    }
  }

  private def handlePhase2bBuffer(
                                   src: Transport#Address,
                                   phase2bBuffer: Phase2bBuffer
                                 ): Unit = {
    metrics.requestsTotal.labels("Phase2bBuffer").inc()
    for (phase2b <- phase2bBuffer.phase2B) {
      processPhase2b(src, phase2b)
    }
  }

  private def handleValueChosenBuffer(
                                       src: Transport#Address,
                                       valueChosenBuffer: ValueChosenBuffer
                                     ): Unit = {
    metrics.requestsTotal.labels("ValueChosenBuffer").inc()
    for (valueChosen <- valueChosenBuffer.valueChosen) {
      val entry = valueChosen.value match {
        case ValueChosen.Value.UniqueId(command) => ECommand(command)
        case ValueChosen.Value.Noop(_)          => ENoop
        case ValueChosen.Value.Empty =>
          leaderLogger.fatal("Empty ValueChosen.Vote")
      }

      log.get(valueChosen.slot) match {
        case Some(existingEntry) =>
          leaderLogger.checkEq(entry, existingEntry)
        case None =>
          log(valueChosen.slot) = entry
      }
    }
    executeLog()
  }

  // Methods ///////////////////////////////////////////////////////////////////

  // Send Phase 1a messages to the acceptors. If thrifty is true, we send with
  // thriftiness. Otherwise, we send to every acceptor.
  private def sendPhase1as(thrifty: Boolean): Unit = {
    for (replica <- replicas) {
      replica.send(
        ReplicaInbound().withPhase1A(
          Phase1a(round = round,
            chosenWatermark = chosenWatermark,
            chosenSlot = log.keysIteratorFrom(chosenWatermark).to[Seq])
        )
      )
    }
  }

  // Given a quorum of phase1b votes, determine a safe value to propose in slot
  // `slot` and a set of other commands that could have been proposed in the
  // slot.
  def chooseProposal(
                      votes: collection.Map[AcceptorId, SortedMap[Slot, Phase1bVote]],
                      slot: Slot
                    ): (Entry, Set[UniqueId]) = {
    def phase1bVoteValueToEntry(voteValue: Phase1bVote.Value): Entry = {
      voteValue match {
        case Value.UniqueId(command) => ECommand(command)
        case Value.Noop(_)          => ENoop
        case Value.Empty =>
          leaderLogger.fatal("Empty Phase1bVote.Value.")
          ???
      }
    }

    val votesInSlot = votes.keys.map(
      (a) =>
        votes(a).get(slot) match {
          case Some(vote) => (vote.voteRound, Some(vote.value))
          case None       => (-1, None)
        }
    )
    val k = votesInSlot.map({ case (voteRound, _) => voteRound }).max
    val V = votesInSlot
      .filter({ case (voteRound, _) => voteRound == k })
      .map({ case (_, voteValue) => voteValue })

    // If no acceptor has voted yet, we're free to propose anything. Here, we
    // propose noop.
    if (k == -1) {
      return (ENoop, Set())
    }

    // If V = {v} is a singleton set, then we must propose v.
    if (V.to[Set].size == 1) {
      return (phase1bVoteValueToEntry(V.head.get), Set())
    }

    // If there exists a v in V such that O4(v), then we must propose it.
    val o4vs = frankenpaxos.Util.popularItems(V, config.f + 1)
    if (o4vs.size > 0) {
      leaderLogger.checkEq(o4vs.size, 1)
      return (phase1bVoteValueToEntry(o4vs.head.get), Set())
    }

    // Otherwise, we can propose anything! Here, we propose one of the values
    // in V, and return the rest in V as also potential proposals.
    //
    // TODO(mwhittaker): Think about whether it is smart to return all the
    // commands seen and not just those in V. We may have to smartly track
    // commands that have already been chosen or are already pending and not
    // re-send them. If acceptors are lagging behind (e.g., because
    // thriftiness), their votes may be very stale.
    (phase1bVoteValueToEntry(V.head.get),
      V.map({
        case Some(Phase1bVote.Value.UniqueId(command)) =>
          command
        case Some(Phase1bVote.Value.Noop(_)) =>
          logger.fatal(s"Noop vote in round $k.")
        case Some(Phase1bVote.Value.Empty) =>
          logger.fatal(s"Empty vote in round $k.")
        case None =>
          logger.fatal(s"None vote in round $k.")
      })
        .to[Set])
  }

  private def processPhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    def toValueChosen(slot: Slot, entry: Entry): ValueChosen = {
      entry match {
        case ECommand(command) =>
          ValueChosen(slot = slot).withUniqueId(command)
        case ENoop => ValueChosen(slot = slot).withNoop(Noop())
      }
    }

    state match {
      case Inactive =>
        leaderLogger.debug(
          s"A leader received a phase 2b response in round ${phase2b.round} " +
            s"from $src but is inactive."
        )

      case Phase1(_, _, _) =>
        leaderLogger.debug(
          s"A leader received a phase 2b response in round ${phase2b.round} " +
            s"from $src but is in phase 1 of round $round."
        )

      case phase2 @ Phase2(pendingEntries,
      phase2bs,
      _,
      _,
      _,
      valueChosenBuffer,
      valueChosenBufferFlushTimer) =>
        def choose(entry: Entry): Unit = {
          log(phase2b.slot) = entry
          pendingEntries -= phase2b.slot
          phase2bs -= phase2b.slot
          executeLog()

          valueChosenBuffer += toValueChosen(phase2b.slot, entry)
          if (valueChosenBuffer.size >= options.valueChosenMaxBufferSize) {
            metrics.valueChosenBufferFullTotal.inc()
            flushValueChosenBuffer()
          }
        }

        // Ignore responses that are not in our current round.
        if (phase2b.round != round) {
          leaderLogger.debug(
            s"A leader received a phase 2b response for round " +
              s"${phase2b.round} from $src but is in round ${round}."
          )
          return
        }

        // Ignore responses for entries that have already been chosen.
        if (log.contains(phase2b.slot)) {
          // Without thriftiness, this is the normal case, so it prints a LOT.
          // So, we comment it out.
          //
          // leaderLogger.debug(
          //   s"A leader received a phase 2b response for slot " +
          //     s"${phase2b.slot} from $src but a value has already been " +
          //     s"chosen in this slot."
          // )
          return
        }

        // Wait for sufficiently many phase2b replies.
        phase2bs.getOrElseUpdate(phase2b.slot, mutable.Map())
        phase2bs(phase2b.slot).put(phase2b.replicaId, phase2b)
        phase2bChosenInSlot(phase2, phase2b.slot) match {
          case NothingReadyYet =>
          // Don't do anything.

          case ClassicReady(entry) =>
            choose(entry)
            metrics.chosenCommandsTotal.labels("classic").inc()

        }
    }
  }

  private def phase2bVoteToEntry(phase2bVote: Phase2b.Vote): Entry = {
    phase2bVote match {
      case Phase2b.Vote.UniqueId(command) => ECommand(command)
      case Phase2b.Vote.Noop(_)          => ENoop
      case Phase2b.Vote.Empty =>
        leaderLogger.fatal("Empty Phase2b.Vote")
        ???
    }
  }

  sealed trait Phase2bVoteResult
  case object NothingReadyYet extends Phase2bVoteResult
  case class ClassicReady(entry: Entry) extends Phase2bVoteResult

  // TODO(mwhittaker): Document.
  private def phase2bChosenInSlot(
                                   phase2: Phase2,
                                   slot: Slot
                                 ): Phase2bVoteResult = {
    val Phase2(pendingEntries, phase2bs, _, _, _, _, _) = phase2

    if (phase2bs
      .getOrElse(slot, mutable.Map())
      .size >= config.f + 1) {
      ClassicReady(pendingEntries(slot))
    } else {
      NothingReadyYet
    }

  }

  def flushPhase2aBuffer(thrifty: Boolean): Unit = {
    state match {
      case Inactive =>
        logger.fatal("Flushing phase2aBuffer while inactive.")

      case Phase1(_, _, _) =>
        logger.fatal("Flushing phase2aBuffer while in phase 1.")

      case Phase2(_, _, _, phase2aBuffer, phase2aBufferFlushTimer, _, _) =>
        if (phase2aBuffer.size > 0) {
          val msg =
            ReplicaInbound().withPhase2ABuffer(Phase2aBuffer(phase2aBuffer))
          replicas.foreach(_.send(msg))
          phase2aBuffer.clear()
          phase2aBufferFlushTimer.reset()
        } else {
          phase2aBufferFlushTimer.reset()
        }
    }
  }

  def flushValueChosenBuffer(): Unit = {
    state match {
      case Inactive =>
        logger.fatal("Flushing valueChosenBuffer while inactive.")

      case Phase1(_, _, _) =>
        logger.fatal("Flushing valueChosenBuffer while in phase 1.")

      case Phase2(_, _, _, _, _, buffer, bufferFlushTimer) =>
        if (buffer.size > 0) {
          buffer.clear()
          bufferFlushTimer.reset()
        } else {
          bufferFlushTimer.reset()
        }
    }
  }

  def resendPhase2as(): Unit = {
    state match {
      case Inactive | Phase1(_, _, _) =>
        leaderLogger.fatal("Executing resendPhase2as not in phase 2.")

      case Phase2(pendingEntries, phase2bs, _, phase2aBuffer, _, _, _) =>
        // It's important that no slot goes forever unchosen. This prevents
        // later slots from executing. Thus, we try and and choose a complete
        // prefix of the log.
        val endSlot: Int = math.max(
          phase2bs.keys.lastOption.getOrElse(-1),
          log.keys.lastOption.getOrElse(-1)
        )

        for (slot <- chosenWatermark to endSlot) {
          val entryToPhase2a: Entry => Phase2a = {
            case ECommand(command) =>
              Phase2a(slot = slot, round = round).withUniqueId(command)
            case ENoop =>
              Phase2a(slot = slot, round = round).withNoop(Noop())
          }

          (log.contains(slot), pendingEntries.get(slot), phase2bs.get(slot)) match {
            case (true, _, _) =>
            // If `slot` is chosen, we don't resend anything.

            case (false, Some(entry), _) =>
              // If we have some pending entry, then we propose that.
              phase2aBuffer.append(entryToPhase2a(entry))

            case (false, None, Some(phase2bsInSlot)) =>
              // If there is no pending entry, then we propose the value with
              // the most votes so far. If no value has been voted, then we
              // just propose Noop.
              val voteValues = phase2bsInSlot.values.map(_.vote)
              val histogram = Util.histogram(voteValues)
              if (voteValues.size == 0) {
                phase2aBuffer += entryToPhase2a(ENoop)
              } else {
                val mostVoted = histogram.maxBy(_._2)._1
                phase2aBuffer += entryToPhase2a(phase2bVoteToEntry(mostVoted))
              }

            case (false, None, None) =>
              // If there is no pending entry and no votes, we propose Noop.
              phase2aBuffer += entryToPhase2a(ENoop)
          }
        }

        flushPhase2aBuffer(thrifty = false)
    }
  }

  def executeLog(): Unit = {
    while (log.contains(chosenWatermark)) {
      log(chosenWatermark) match {
        case ECommand(
        UniqueId(clientAddressBytes, clientPseudonym, clientId)
        ) =>
          val clientAddress = transport.addressSerializer.fromBytes(
            clientAddressBytes.toByteArray()
          )

          // True if this command has already been executed.
          val executed =
            clientTable.get((clientAddress, clientPseudonym)) match {
              case Some((highestClientId, _)) => clientId <= highestClientId
              case None                       => false
            }

          if (!executed) {
            val output = stateMachine.run(clientAddressBytes.toByteArray())
            clientTable((clientAddress, clientPseudonym)) = (clientId, output)
            metrics.executedCommandsTotal.inc()

            // Note that only the leader replies to the client since
            // ProposeReplies include the round of the leader, and only the
            // leader knows this.
            if (state != Inactive) {
              val client =
                chan[Client[Transport]](clientAddress, Client.serializer)
              client.send(
                ClientInbound().withClientReply(
                  ClientReply(
                    clientId = clientId,
                    clientPseudonym = clientPseudonym,
                    result = ByteString.copyFrom(output))
                )
              )
            }
          } else {
            metrics.repeatedCommandsTotal.inc()
          }
        case ENoop =>
          // Do nothing.
          metrics.executedNoopsTotal.inc()
      }
      chosenWatermark += 1
    }
  }
}
