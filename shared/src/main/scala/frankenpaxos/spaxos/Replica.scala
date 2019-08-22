package frankenpaxos.spaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.clienttable.ClientTable
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.thrifty.ThriftySystem
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object ReplicaInboundSerializer extends ProtoSerializer[ReplicaInbound] {
  type A = ReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class ReplicaOptions(
    thriftySystem: ThriftySystem,
    resendPreAcceptsTimerPeriod: java.time.Duration,
    defaultToSlowPathTimerPeriod: java.time.Duration,
    resendAcceptsTimerPeriod: java.time.Duration,
    resendPreparesTimerPeriod: java.time.Duration,
    recoverInstanceTimerMinPeriod: java.time.Duration,
    recoverInstanceTimerMaxPeriod: java.time.Duration,
    // If `unsafeSkipGraphExecution` is true, replicas skip graph execution
    // entirely. Instead, they execute commands as soon as they are committed.
    //
    // As the name suggests, this is not safe. It completely breaks the
    // protocol. This flag should only be used to debug performance issues. For
    // example, disabling graph execution makes it easier to see if graph
    // execution is a bottleneck.
    //
    // Note that if unsafeSkipGraphExecution is true, then
    // executeGraphBatchSize and executeGraphTimerPeriod are completely
    // ignored.
    unsafeSkipGraphExecution: Boolean,
    // When a replica receives a committed command, it adds it to its
    // dependency graph. When executeGraphBatchSize commands have been
    // committed, the replica attempts to execute as many commands in the graph
    // as possible. If executeGraphBatchSize commands have not been committed
    // within executeGraphTimerPeriod since the last time the graph was
    // executed, the graph is executed.
    executeGraphBatchSize: Int,
    executeGraphTimerPeriod: java.time.Duration
)

@JSExportAll
object ReplicaOptions {
  val default = ReplicaOptions(
    thriftySystem = ThriftySystem.NotThrifty,
    resendPreAcceptsTimerPeriod = java.time.Duration.ofSeconds(1),
    defaultToSlowPathTimerPeriod = java.time.Duration.ofSeconds(1),
    resendAcceptsTimerPeriod = java.time.Duration.ofSeconds(1),
    resendPreparesTimerPeriod = java.time.Duration.ofSeconds(1),
    recoverInstanceTimerMinPeriod = java.time.Duration.ofMillis(500),
    recoverInstanceTimerMaxPeriod = java.time.Duration.ofMillis(1500),
    unsafeSkipGraphExecution = false,
    executeGraphBatchSize = 1,
    executeGraphTimerPeriod = java.time.Duration.ofSeconds(1)
  )
}

@JSExportAll
class ReplicaMetrics(collectors: Collectors) {
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
object Replica {
  val serializer = ReplicaInboundSerializer

  type ReplicaIndex = Int
  type ClientPseudonym = Int
  type ClientId = Int

  // The special null ballot. Replicas set their vote ballot to nullBallot to
  // indicate that they have not yet voted.
  val nullBallot = Ballot(-1, -1)

  // A command (or noop) along with its sequence number and dependencies. In
  // the EPaxos paper, these triples are denoted like this:
  //
  //                      (\gamma, seq_\gamma, deps_\gamma)
  @JSExportAll
  case class CommandTriple(
      commandOrNoop: CommandOrNoop,
      sequenceNumber: Int,
      dependencies: Set[Instance]
  )

// The core data structure of every EPaxos replica, the cmd log, records
// information about every instance that a replica knows about. In the EPaxos
// paper, the cmd log is visualized as an infinite two-dimensional array that
// looks like this:
//
//      ... ... ...
//     |___|___|___|
//   2 |   |   |   |
//     |___|___|___|
//   1 |   |   |   |
//     |___|___|___|
//   0 |   |   |   |
//     |___|___|___|
//       Q   R   S
//
// The array is indexed on the left by an instance number and on the bottom
// by a replica id. Thus, every cell is indexed by an instance (e.g. Q.1),
// and every cell contains the state of the instance that indexes it.
// `CmdLogEntry` represents the data within a cell, and `cmdLog` represents
// the cmd log.
//
// Note that EPaxos has a small bug in how it implements ballots. The EPaxos
// TLA+ specification and Go implementation have a single ballot per command
// log entry. As detailed in [1], this is a bug. We need two ballots, like
// what is done in normal Paxos. Note that we haven't proven this two-ballot
// implementation is correct, so it may also be wrong.
//
// [1]: https://drive.google.com/open?id=1dQ_cigMWJ7w9KAJeSYcH3cZoFpbraWxm
  @JSExportAll
  sealed trait CmdLogEntry


}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    // Public for the JS visualizations.
    val stateMachine: StateMachine,
    options: ReplicaOptions = ReplicaOptions.default,
    metrics: ReplicaMetrics = new ReplicaMetrics(PrometheusCollectors)
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
      pendingProposals: mutable.Buffer[(Transport#Address, ProposeRequest)],
      // A timer to resend phase 1as.
      resendPhase1as: Transport#Timer
  ) extends State

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
  val cmdLog: mutable.Map[Instance, CmdLogEntry] =
    mutable.Map[Instance, CmdLogEntry]()

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

  // Every replica maintains a local instance number i, initially 0. When a
  // replica R receives a command, it assigns the command instance R.i and then
  // increments i. Thus, every replica fills in the cmd log vertically within
  // its column from bottom to top. `nextAvailableInstance` represents i.
  @JSExport
  protected var nextAvailableInstance: Int = 0

  // The default fast path ballot used by this replica.
  @JSExport
  protected val defaultBallot: Ballot = Ballot(0, index)

  // The largest ballot ever seen by this replica. largestBallot is used when a
  // replica receives a nack and needs to choose a larger ballot.
  @JSExport
  protected var largestBallot: Ballot = Ballot(0, index)

  // The number of committed commands that are in the graph that have not yet
  // been processed. We process the graph every `options.executeGraphBatchSize`
  // committed commands and every `options.executeGraphTimerPeriod` seconds. If
  // the timer expires, we clear this number.
  @JSExport
  protected var numPendingCommittedCommands: Int = 0


  // The client table records the response to the latest request from each
  // client. For example, if command c1 sends command x with id 2 to a leader
  // and the leader later executes x yielding response y, then c1 maps to (2,
  // y) in the client table.
  @JSExport
  protected val clientTable =
    new ClientTable[(Transport#Address, ClientPseudonym), Array[Byte]]()

  @JSExport
  protected val leaderStates = mutable.Map[Instance, LeaderState]()

  // An index used to efficiently compute dependencies and sequence numbers.
  // Note that noops are not entered into the conflict index.
  @JSExport
  protected val conflictIndex = stateMachine.conflictIndex[Instance]()

  // If a replica commits a command in instance I with a dependency on
  // uncommitted instance J, then the replica sets a timer to recover instance
  // J. This prevents an instance from being forever stalled.
  @JSExport
  protected val recoverInstanceTimers = mutable.Map[Instance, Transport#Timer]()

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    import ReplicaInbound.Request
    inbound.request match {
      case Request.ClientRequest(r) => handleClientRequest(src, r)
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      request: ClientRequest
  ): Unit = {
    metrics.requestsTotal.labels("ClientRequest").inc()

    // If we already have a response to this request in the client table, we
    // simply return it.
    val clientIdentity = (src, request.command.clientPseudonym)
    clientTable.executed(clientIdentity, request.command.clientId) match {
      case ClientTable.NotExecuted =>
      // Not executed yet, we'll have to get it chosen.

      case ClientTable.Executed(None) =>
        // Executed already but a stale command. We ignore this request.
        return

      case ClientTable.Executed(Some(output)) =>
        // Executed already and is the most recent command. We relay the
        // response to the client.
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound()
            .withClientReply(
              ClientReply(clientPseudonym = request.command.clientPseudonym,
                          clientId = request.command.clientId,
                          result = ByteString.copyFrom(output))
            )
        )
        return
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

    // Make sure we should be processing this message at all. For example,
    // sometimes we nack it, sometimes we ignore it, sometimes we re-send
    // replies that we previously sent because of it.
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    requests.add(forward.clientRequest)
    replica.send(ReplicaInbound().withAcknowledge(Acknowledge(forward.clientRequest.uniqueId)))
  }

  private def handleAcknowledge(
      src: Transport#Address,
      acknowledge: Acknowledge
  ): Unit = {
    metrics.requestsTotal.labels("Acknowledge").inc()

    // Make sure we should be processing this message at all. For example,
    // sometimes we nack it, sometimes we ignore it, sometimes we re-send
    // replies that we previously sent because of it.
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    acks.put(acknowledge.uniqueId, acks.getOrElse(acknowledge.uniqueId, 0) + 1)

    val f: Int = 1
    if (acks.getOrElse(acknowledge.uniqueId, 0) >= f + 1) {
      stableIds.add(acknowledge.uniqueId)
      // This replica is the leader
      if (leaderIndex == index && !proposed.contains(acknowledge.uniqueId)) {
        proposed.add(acknowledge.uniqueId)
        for (replica <- replicas) {
          replica.send(ReplicaInbound().withProposeRequest(0, acknowledge.uniqueId))
        }
      }
    }
  }

  private def handleProposeRequest(
      src: Transport#Address,
      request: ProposeRequest
  ): Unit = {
    metrics.requestsTotal.labels("ProposeRequest").inc()
    val client = chan[Client[Transport]](src, Client.serializer)

    // TODO(mwhittaker): Ignore requests that are older than the current id
    // stored in the client table.

    // If we've cached the result of this proposed command in the client table,
    // then we can reply to the client directly. Note that only the leader
    // replies to the client since ProposeReplies include the round of the
    // leader, and only the leader knows this.
    clientTable.get((src, request.command.clientPseudonym)) match {
      case Some((clientId, result)) =>
        if (request.command.clientId == clientId && state != Inactive) {
          leaderLogger.debug(
            s"The latest command for client $src pseudonym " +
              s"${request.command.clientPseudonym} (i.e., command $clientId)" +
              s"was found in the client table."
          )
          client.send(
            ClientInbound().withProposeReply(
              ProposeReply(round = round,
                           clientPseudonym = request.command.clientPseudonym,
                           clientId = clientId,
                           result = ByteString.copyFrom(result))
            )
          )
          return
        }
      case None =>
    }

    state match {
      case Inactive =>
        leaderLogger.debug(
          s"Leader received propose request from $src but is inactive."
        )

      case Phase1(_, pendingProposals, _) =>
        if (request.round != round) {
          // We don't want to process requests from out of date clients.
          leaderLogger.debug(
            s"Leader received a propose request from $src in round " +
              s"${request.round}, but is in round $round. Sending leader info."
          )
          client.send(ClientInbound().withLeaderInfo(LeaderInfo(round)))
        } else {
          // We buffer all pending proposals in phase 1 and process them later
          // when we enter phase 2.
          pendingProposals += ((src, request))
        }

      case Phase2(pendingEntries,
                  phase2bs,
                  _,
                  phase2aBuffer,
                  phase2aBufferFlushTimer,
                  _,
                  _) =>
        if (request.round != round) {
          // We don't want to process requests from out of date clients.
          leaderLogger.debug(
            s"Leader received a propose request from $src in round " +
              s"${request.round}, but is in round $round. Sending leader info."
          )
          client.send(ClientInbound().withLeaderInfo(LeaderInfo(round)))
          return
        }

            phase2aBuffer += Phase2a(slot = nextSlot, round = round)
              .withCommand(request.command)
            pendingEntries(nextSlot) = ECommand(request.command)
            phase2bs(nextSlot) = mutable.Map()
            nextSlot += 1
            if (phase2aBuffer.size >= options.phase2aMaxBufferSize) {
              metrics.phase2aBufferFullTotal.inc()
              flushPhase2aBuffer(thrifty = true)
            }

    }
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
        phase1bs(request.acceptorId) = request
        if (phase1bs.size < config.classicQuorumSize) {
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
        val proposedCommands = mutable.Set[Command]()
        val yetToProposeCommands = mutable.Set[Command]()
        for (slot <- chosenWatermark to endSlot) {
          val (proposal, commands) = chooseProposal(votes, slot)
          yetToProposeCommands ++= commands

          val phase2a = proposal match {
            case ECommand(command) =>
              proposedCommands += command
              Phase2a(slot = slot, round = round).withCommand(command)
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
        for ((_, proposal) <- pendingProposals) {
          phase2aBuffer += Phase2a(slot = nextSlot, round = round)
            .withCommand(proposal.command)
          pendingEntries(nextSlot) = ECommand(proposal.command)
          phase2bs(nextSlot) = mutable.Map()
          nextSlot += 1
        }

        // Replay the safe to propose values that we haven't just proposed.
        for (command <- yetToProposeCommands.diff(proposedCommands)) {
          phase2aBuffer += Phase2a(slot = nextSlot, round = round)
            .withCommand(command)
          pendingEntries(nextSlot) = ECommand(command)
          phase2bs(nextSlot) = mutable.Map()
          nextSlot += 1
        }

        // If this is a fast round, send a suffix of anys.
        if (config.roundSystem.roundType(round) == FastRound) {
          phase2aBuffer +=
            Phase2a(slot = nextSlot, round = round)
              .withAnySuffix(AnyValSuffix())
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
          leaderChange(address, nack.round)
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
        case ValueChosen.Value.Command(command) => ECommand(command)
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

}
