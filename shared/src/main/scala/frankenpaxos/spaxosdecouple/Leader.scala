package frankenpaxos.spaxosdecouple

import collection.immutable.SortedMap
import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.election.LeaderElectionOptions
import frankenpaxos.heartbeat.HeartbeatOptions
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.roundsystem.ClassicRound
import frankenpaxos.roundsystem.FastRound
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.thrifty.ThriftySystem
import scala.collection.breakOut
import scala.scalajs.js.annotation._

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
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
                          leaderElectionOptions: LeaderElectionOptions,
                          heartbeatOptions: HeartbeatOptions
                        )

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    thriftySystem = ThriftySystem.Closest,
    resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(5),
    resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(5),
    phase2aMaxBufferSize = 25,
    phase2aBufferFlushPeriod = java.time.Duration.ofMillis(100),
    valueChosenMaxBufferSize = 100,
    valueChosenBufferFlushPeriod = java.time.Duration.ofSeconds(5),
    leaderElectionOptions = LeaderElectionOptions.default,
    heartbeatOptions = HeartbeatOptions.default
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_leader_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
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

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer

  sealed trait Entry
  case class ECommand(uniqueId: UniqueId) extends Entry
  object ENoop extends Entry
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
                                                              address: Transport#Address,
                                                              transport: Transport,
                                                              logger: Logger,
                                                              config: Config[Transport],
                                                              // Public for Javascript.
                                                              val stateMachine: StateMachine,
                                                              options: LeaderOptions = LeaderOptions.default,
                                                              metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
                                                            ) extends Actor(address, transport, logger) {
  import Leader._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override val serializer = LeaderInboundSerializer

  type AcceptorId = Int
  type Round = Int
  type Slot = Int
  type ClientPseudonym = Int
  type ClientId = Int

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the leader's id.
  logger.check(config.leaderAddresses.contains(address))
  private val leaderId = config.leaderAddresses.indexOf(address)

  // Channels to all other leaders.
  private val otherLeaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses if a != address)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Channels to executors
  private val executors: Seq[Chan[Executor[Transport]]] =
    for (a <- config.executorAddresses)
      yield chan[Executor[Transport]](a, Executor.serializer)

  // Channels to fake leaders
  private val fakeLeaders: Seq[Chan[FakeLeader[Transport]]] =
    for (a <- config.fakeLeaderAddresses)
      yield chan[FakeLeader[Transport]](a, FakeLeader.serializer)

  // Channels to all the acceptors.
  private val acceptorsByAddress
  : Map[Transport#Address, Chan[Acceptor[Transport]]] = {
    for (address <- config.acceptorAddresses)
      yield (address -> chan[Acceptor[Transport]](address, Acceptor.serializer))
  }.toMap

  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    acceptorsByAddress.values.toSeq

  // The current round. Initially, the leader that owns round 0 is the active
  // leader, and all other leaders are inactive.
  @JSExport
  protected var round: Round =
  if (config.roundSystem.leader(0) == leaderId) 0 else -1

  // The log of chosen commands. Public for testing.
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
  metrics.chosenWatermark.set(chosenWatermark)

  // The next slot in which to propose a command.
  //
  // TODO(mwhittaker): Add a buffer to prevent the leader from running too far
  // ahead.
  @JSExport
  protected var nextSlot: Slot = 0
  metrics.nextSlot.set(nextSlot)

  // Leaders participate in a leader election protocol to maintain a
  // (hopefully) stable leader.
  @JSExport
  protected val electionAddress: Transport#Address =
  config.leaderElectionAddresses(leaderId)
  @JSExport
  protected val election: frankenpaxos.election.Participant[Transport] =
    new frankenpaxos.election.Participant[Transport](
      electionAddress,
      transport,
      logger,
      config.leaderElectionAddresses.to[Set],
      leader = Some(
        config.leaderElectionAddresses(config.roundSystem.leader(0))
      ),
      options.leaderElectionOptions
    )

  // TODO(mwhittaker): Is this thread safe? It's possible that the election
  // participant invokes the callback before this leader has finished
  // initializing?
  election.register((address) => {
    // The address returned by the election participant is the address of the
    // election participant, not of the leader.
    val leaderAddress =
    config.leaderAddresses(config.leaderElectionAddresses.indexOf(address))
    leaderChange(leaderAddress, round)
  })

  // Leaders monitor acceptors to make sure they are still alive.
  @JSExport
  protected val heartbeatAddress: Transport#Address =
  config.leaderHeartbeatAddresses(leaderId)
  @JSExport
  protected val heartbeat: frankenpaxos.heartbeat.Participant[Transport] =
    new frankenpaxos.heartbeat.Participant[Transport](
      address = heartbeatAddress,
      transport = transport,
      logger = logger,
      addresses = config.acceptorHeartbeatAddresses,
      options = options.heartbeatOptions
    )

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
                     pendingProposals: mutable.Set[(Transport#Address, Proposal)],
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

  private val resendPhase2asTimer: Transport#Timer = timer(
    "resendPhase2as",
    options.resendPhase2asTimerPeriod,
    () => {
      //resendPhase2as()
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
      //flushValueChosenBuffer()
      metrics.valueChosenBufferFlushTotal.inc()
    }
  )

  @JSExport
  protected var state: State =
    if (round == 0) {
      sendPhase1as(thrifty = true)
      resendPhase1asTimer.start()
      Phase1(mutable.Map(), mutable.Set(), resendPhase1asTimer)
    } else {
      Inactive
    }

  @JSExport
  protected val pendingIds: mutable.Set[UniqueId] = mutable.Set[UniqueId]()

  @JSExport
  protected val chosenIds: mutable.Set[UniqueId] = mutable.Set[UniqueId]()

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

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.Proposal(r)    => handleProposal(src, r)
      case Request.Phase1B(r)           => handlePhase1b(src, r)
      case Request.Phase1BNack(r)       => handlePhase1bNack(src, r)
      //case Request.Phase2BBuffer(r)     => handlePhase2bBuffer(src, r)
      case Request.ValueChosenBuffer(r) => handleValueChosenBuffer(src, r)
      case Request.Empty =>
        leaderLogger.fatal("Empty LeaderInbound encountered.")
    }

    metrics.chosenWatermark.set(chosenWatermark)
    metrics.nextSlot.set(nextSlot)
  }

  private def handleProposal(
                                    src: Transport#Address,
                                    proposal: Proposal
                                  ): Unit = {
    metrics.requestsTotal.labels("ProposeRequest").inc()
    val client = chan[Client[Transport]](src, Client.serializer)

    // TODO(mwhittaker): Ignore requests that are older than the current id
    // stored in the client table.

    // If we've cached the result of this proposed command in the client table,
    // then we can reply to the client directly. Note that only the leader
    // replies to the client since ProposeReplies include the round of the
    // leader, and only the leader knows this.
    clientTable.get((src, proposal.uniqueId.clientPseudonym)) match {
      case Some((clientId, result)) =>
        if (proposal.uniqueId.clientId == clientId && state != Inactive) {
          leaderLogger.debug(
            s"The latest command for client $src pseudonym " +
              s"${proposal.uniqueId.clientPseudonym} (i.e., command $clientId)" +
              s"was found in the client table."
          )
          client.send(
            ClientInbound().withClientReply(
              ClientReply(
                clientPseudonym = proposal.uniqueId.clientPseudonym,
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
        if (proposal.round != round) {
            // We don't want to process requests from out of date clients.
          leaderLogger.debug(
            s"Leader received a propose request from $src in round " +
              s"${proposal.round}, but is in round $round. Sending leader info."
          )
          val proposer = chan[Proposer[Transport]](src, Proposer.serializer)
          proposer.send(ProposerInbound().withLeaderInfo(LeaderInfo(round)))
        } else {
          // We buffer all pending proposals in phase 1 and process them later
          // when we enter phase 2.
          pendingProposals += ((src, proposal))
        }

      case Phase2(pendingEntries,
      phase2bs,
      _,
      phase2aBuffer,
      phase2aBufferFlushTimer,
      _,
      _) =>
        if (proposal.round != round) {
          // We don't want to process requests from out of date proposers.
          leaderLogger.debug(
            s"Leader received a propose request from $src in round " +
              s"${proposal.round}, but is in round $round. Sending leader info."
          )
          val proposer =
            chan[Proposer[Transport]](src, Proposer.serializer)
          proposer.send(ProposerInbound().withLeaderInfo(LeaderInfo(round)))
          return
        }


        config.roundSystem.roundType(round) match {
          case ClassicRound =>
            if (pendingIds.contains(proposal.uniqueId) || chosenIds.contains(proposal.uniqueId)) {
              return
            }
            phase2aBuffer += Phase2a(slot = nextSlot, round = round)
              .withUniqueId(proposal.uniqueId)
            pendingEntries(nextSlot) = ECommand(proposal.uniqueId)
            phase2bs(nextSlot) = mutable.Map()
            pendingIds.add(proposal.uniqueId)
            nextSlot += 1
            if (phase2aBuffer.size >= options.phase2aMaxBufferSize) {
              metrics.phase2aBufferFullTotal.inc()
              flushPhase2aBuffer(thrifty = true)
            }

          case FastRound =>
            // If we're in a fast round, and the client knows we're in a fast
            // round (because request.round == round), then the client should
            // not be sending the leader any requests. It only does so if there
            // was a failure. In this case, we change to a higher round.
            leaderLogger.debug(
              s"Leader received a client request from $src during a fast " +
                s"round. We are increasing our round."
            )
            leaderChange(address, round)
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
        if (phase1bs.size < config.quorumSize) {
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
        for ((_, proposal) <- pendingProposals) {
          phase2aBuffer += Phase2a(slot = nextSlot, round = round)
            .withUniqueId(proposal.uniqueId)
          pendingEntries(nextSlot) = ECommand(proposal.uniqueId)
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
          leaderChange(address, nack.round)
        }
    }
  }

  /*private def handlePhase2bBuffer(
                                   src: Transport#Address,
                                   phase2bBuffer: Phase2bBuffer
                                 ): Unit = {
    metrics.requestsTotal.labels("Phase2bBuffer").inc()
    for (phase2b <- phase2bBuffer.phase2B) {
      processPhase2b(src, phase2b)
    }
  }*/

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

      pendingIds.remove(valueChosen.getUniqueId)
      chosenIds.add(valueChosen.getUniqueId)
    }
    //executeLog()
    while (log.contains(chosenWatermark)) {
      chosenWatermark += 1
    }
  }

  // Methods ///////////////////////////////////////////////////////////////////
  def quorumSize(round: Round): Int = {
    /*config.roundSystem.roundType(round) match {
      case FastRound    => config.fastQuorumSize
      case ClassicRound => config.classicQuorumSize
    }*/
    config.quorumSize
  }

  // thriftyAcceptors(min) uses `options.thriftySystem` to thriftily select at
  // least `min` acceptors.
  def thriftyAcceptors(min: Int): Set[Chan[Acceptor[Transport]]] = {
    // The addresses returned by the heartbeat node are heartbeat addresses.
    // We have to transform them into acceptor addresses.
    options.thriftySystem
      .choose(heartbeat.unsafeNetworkDelay, min)
      .map(config.acceptorHeartbeatAddresses.indexOf(_))
      .map(config.acceptorAddresses(_))
      .map(acceptorsByAddress(_))
  }

  // Send Phase 1a messages to the acceptors. If thrifty is true, we send with
  // thriftiness. Otherwise, we send to every acceptor.
  private def sendPhase1as(thrifty: Boolean): Unit = {
    val acceptors = if (thrifty) {
      thriftyAcceptors(config.quorumSize)
    } else {
      this.acceptors
    }

    for (acceptor <- acceptors) {
      acceptor.send(
        AcceptorInbound().withPhase1A(
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
      import Phase1bVote.Value
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
    val o4vs = frankenpaxos.Util.popularItems(V, config.quorumSize)
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

  /*private def processPhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
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
          //executeLog()
          while (log.contains(chosenWatermark)) {
            chosenWatermark += 1
          }
          //executors.foreach(_.send(ExecutorInbound().withValueChosen(toValueChosen(phase2b.slot, entry))))

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
        phase2bs(phase2b.slot).put(phase2b.acceptorId, phase2b)
        phase2bChosenInSlot(phase2, phase2b.slot) match {
          case NothingReadyYet =>
          // Don't do anything.

          case ClassicReady(entry) =>
            choose(entry)
            metrics.chosenCommandsTotal.labels("classic").inc()

          case FastReady(entry) =>
            choose(entry)
            metrics.chosenCommandsTotal.labels("fast").inc()

          case FastStuck =>
            // The fast round is stuck, so we start again in a higher round.
            // TODO(mwhittaker): We might want to have all stuck things pending
            // for the next round.
            leaderLogger.debug(
              s"Slot ${phase2b.slot} is stuck. Changing to a higher round."
            )
            leaderChange(address, round)
            metrics.stuckTotal.inc()
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
  case class FastReady(entry: Entry) extends Phase2bVoteResult
  case object FastStuck extends Phase2bVoteResult

  // TODO(mwhittaker): Document.
  private def phase2bChosenInSlot(
                                   phase2: Phase2,
                                   slot: Slot
                                 ): Phase2bVoteResult = {
    val Phase2(pendingEntries, phase2bs, _, _, _, _, _) = phase2

    config.roundSystem.roundType(round) match {
      case ClassicRound =>
        if (phase2bs
          .getOrElse(slot, mutable.Map())
          .size >= config.quorumSize) {
          ClassicReady(pendingEntries(slot))
        } else {
          NothingReadyYet
        }

      case FastRound =>
        phase2bs.getOrElseUpdate(slot, mutable.Map())
        if (phase2bs(slot).size < config.quorumSize) {
          return NothingReadyYet
        }

        val voteValueCounts = Util.histogram(phase2bs(slot).values.map(_.vote))

        // We've heard from `phase2bs(slot).size` acceptors. This means there
        // are `votesLeft = config.n - phase2bs(slot).size` acceptors left. In
        // order for a value to be choosable, it must be able to reach a fast
        // quorum of votes if all the `votesLeft` acceptors vote for it. If no
        // such value exists, no value can be chosen.
        val votesLeft = config.n - phase2bs(slot).size
        if (!voteValueCounts.exists({
          case (_, count) => count + votesLeft >= config.quorumSize
        })) {
          leaderLogger.debug(
            s"Slot $slot stuck with following histogram: $voteValueCounts."
          )
          return FastStuck
        }

        for ((voteValue, count) <- voteValueCounts) {
          if (count >= config.quorumSize) {
            return FastReady(phase2bVoteToEntry(voteValue))
          }
        }

        NothingReadyYet
    }
  }*/

  def flushPhase2aBuffer(thrifty: Boolean): Unit = {
    state match {
      case Inactive =>
        logger.fatal("Flushing phase2aBuffer while inactive.")

      case Phase1(_, _, _) =>
        logger.fatal("Flushing phase2aBuffer while in phase 1.")

      case Phase2(_, _, _, phase2aBuffer, phase2aBufferFlushTimer, _, _) =>
        if (phase2aBuffer.size > 0) {
          /*val msg =
            AcceptorInbound().withPhase2ABuffer(Phase2aBuffer(phase2aBuffer))
          if (thrifty) {
            thriftyAcceptors(quorumSize(round)).foreach(_.send(msg))
          } else {
            acceptors.foreach(_.send(msg))
          }*/
          val r = scala.util.Random
          val rand = r.nextInt(fakeLeaders.size)
          fakeLeaders(rand).send(FakeLeaderInbound().withPhase2ABuffer(Phase2aBuffer(phase2aBuffer)))
          phase2aBuffer.clear()
          phase2aBufferFlushTimer.reset()
        } else {
          phase2aBufferFlushTimer.reset()
        }
    }
  }

  /*def flushValueChosenBuffer(): Unit = {
    state match {
      case Inactive =>
        logger.fatal("Flushing valueChosenBuffer while inactive.")

      case Phase1(_, _, _) =>
        logger.fatal("Flushing valueChosenBuffer while in phase 1.")

      case Phase2(_, _, _, _, _, buffer, bufferFlushTimer) =>
        if (buffer.size > 0) {
          /*otherLeaders.foreach(
            _.send(
              LeaderInbound().withValueChosenBuffer(ValueChosenBuffer(buffer))
            )
          )*/
          executors.foreach(_.send(ExecutorInbound().withValueChosenBuffer(ValueChosenBuffer(buffer))))
          buffer.clear()
          bufferFlushTimer.reset()
        } else {
          bufferFlushTimer.reset()
        }
    }
  }*/

  /*def resendPhase2as(): Unit = {
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
  }*/

  // Switch over to a new leader. If the new leader is ourselves, then we
  // increase our round and enter a new round larger than `higherThanRound`.
  def leaderChange(leader: Transport#Address, higherThanRound: Int): Unit = {
    leaderLogger.checkGe(higherThanRound, round)
    metrics.leaderChangesTotal.inc()

    // Try to go to a fast round if we think that a fast quorum of acceptors
    // are alive. If we think fewer than a fast quorum of acceptors are alive,
    // then proceed to a classic round.
    val nextRound = if (heartbeat.unsafeAlive().size >= config.quorumSize) {
      config.roundSystem
        .nextFastRound(leaderId, higherThanRound)
        .getOrElse(
          config.roundSystem.nextClassicRound(leaderId, higherThanRound)
        )
    } else {
      config.roundSystem.nextClassicRound(leaderId, higherThanRound)
    }

    (state, leader == address) match {
      case (Inactive, true) =>
        // We are the new leader!
        leaderLogger.debug(
          s"Leader $address was inactive, but is now the leader."
        )
        round = nextRound
        sendPhase1as(true)
        resendPhase1asTimer.start()
        state = Phase1(mutable.Map(), mutable.Set(), resendPhase1asTimer)

      case (Inactive, false) =>
        // Don't do anything. We're still not the leader.
        leaderLogger.debug(s"Leader $address was inactive and still is.")

      case (Phase1(_, _, resendPhase1asTimer), true) =>
        // We were and still are the leader, but in a higher round.
        leaderLogger.debug(s"Leader $address was the leader and still is.")
        round = nextRound
        sendPhase1as(true)
        resendPhase1asTimer.reset()
        state = Phase1(mutable.Map(), mutable.Set(), resendPhase1asTimer)

      case (Phase1(_, _, resendPhase1asTimer), false) =>
        // We are no longer the leader!
        leaderLogger.debug(s"Leader $address was the leader, but no longer is.")
        resendPhase1asTimer.stop()
        state = Inactive

      case (Phase2(_,
      _,
      resendPhase2asTimer,
      _,
      phase2aBufferFlushTimer,
      _,
      valueChosenBufferFlushTimer),
      true) =>
        // We were and still are the leader, but in a higher round.
        leaderLogger.debug(s"Leader $address was the leader and still is.")
        resendPhase2asTimer.stop()
        phase2aBufferFlushTimer.stop()
        valueChosenBufferFlushTimer.stop()
        round = nextRound
        sendPhase1as(true)
        resendPhase1asTimer.start()
        state = Phase1(mutable.Map(), mutable.Set(), resendPhase1asTimer)

      case (Phase2(_,
      _,
      resendPhase2asTimer,
      _,
      phase2aBufferFlushTimer,
      _,
      valueChosenBufferFlushTimer),
      false) =>
        // We are no longer the leader!
        leaderLogger.debug(s"Leader $address was the leader, but no longer is.")
        resendPhase2asTimer.stop()
        phase2aBufferFlushTimer.stop()
        valueChosenBufferFlushTimer.stop()
        state = Inactive
    }
  }

  /*def executeLog(): Unit = {
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
            //val output = stateMachine.run(command.toByteArray())
            val output = clientAddressBytes.toByteArray
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
                    clientPseudonym = clientPseudonym,
                    clientId = clientId,
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
  }*/
}
