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
object FakeLeaderInboundSerializer extends ProtoSerializer[FakeLeaderInbound] {
  type A = FakeLeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class FakeLeaderOptions(
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
object FakeLeaderOptions {
  val default = FakeLeaderOptions(
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
class FakeLeaderMetrics(collectors: Collectors) {
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
object FakeLeader {
  val serializer = FakeLeaderInboundSerializer

  sealed trait Entry
  case class ECommand(uniqueId: UniqueId) extends Entry
  object ENoop extends Entry
}

@JSExportAll
class FakeLeader[Transport <: frankenpaxos.Transport[Transport]](
                                                              address: Transport#Address,
                                                              transport: Transport,
                                                              logger: Logger,
                                                              config: Config[Transport],
                                                              // Public for Javascript.
                                                              options: FakeLeaderOptions = FakeLeaderOptions.default,
                                                              metrics: FakeLeaderMetrics = new FakeLeaderMetrics(PrometheusCollectors)
                                                            ) extends Actor(address, transport, logger) {
  import FakeLeader._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = FakeLeaderInbound
  override val serializer = FakeLeaderInboundSerializer

  type AcceptorId = Int
  type Round = Int
  type Slot = Int
  type ClientPseudonym = Int
  type ClientId = Int

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the leader's id.
  logger.check(config.leaderAddresses.contains(address))
  private val leaderId = config.leaderAddresses.indexOf(address)

  // Channels to all real leaders.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Channels to executors
  private val executors: Seq[Chan[Executor[Transport]]] =
    for (a <- config.executorAddresses)
      yield chan[Executor[Transport]](a, Executor.serializer)

  // Channels to all the acceptors.
  private val acceptorsByAddress
  : Map[Transport#Address, Chan[Acceptor[Transport]]] = {
    for (address <- config.acceptorAddresses)
      yield (address -> chan[Acceptor[Transport]](address, Acceptor.serializer))
  }.toMap

  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    acceptorsByAddress.values.toSeq

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

  // The state of the leader.
  @JSExportAll
  sealed trait State

  // This fake leader is now executing phase 2.
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


  @JSExport
  protected var state: State = Phase2(mutable.SortedMap[Slot, Entry](), mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]](),
    resendPhase2asTimer, mutable.Buffer[Phase2a](), phase2aBufferFlushTimer, mutable.Buffer[ValueChosen](), valueChosenBufferFlushTimer)

  // Custom logger.
  val leaderLogger = new Logger(frankenpaxos.LogDebug) {
    private def withInfo(s: String): String = {
      val stateString = state match {
        case Phase2(_, _, _, _, _, _, _) => "Phase 2"
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
    import FakeLeaderInbound.Request
    inbound.request match {
      case Request.Phase2ABuffer(r)    => handlePhase2aBuffer(src, r)
      case Request.Phase2BBuffer(r)     => handlePhase2bBuffer(src, r)
      case Request.Empty =>
        leaderLogger.fatal("Empty FakeLeaderInbound encountered.")
    }

  }

  private def handlePhase2aBuffer(
                              src: Transport#Address,
                              phase2aBuffer: Phase2aBuffer
                            ): Unit = {
    state match {
      case Phase2(pendingEntries,
      phase2bs,
      _,
      phase2aBuffer,
      phase2aBufferFlushTimer,
      _,
      _) =>
        round = phase2aBuffer.head.round
        config.roundSystem.roundType(round) match {
          case ClassicRound =>
            for (phase2a <- phase2aBuffer) {
              phase2aBuffer += phase2a
              if (phase2a.value.isUniqueId) {
                pendingEntries(phase2a.slot) = ECommand(phase2a.value.uniqueId.get)
              }

              if (phase2a.value.isNoop) {
                pendingEntries(phase2a.slot) = ENoop
              }
              phase2bs(phase2a.slot) = mutable.Map()
            }

            if (phase2aBuffer.size >= options.phase2aMaxBufferSize) {
              metrics.phase2aBufferFullTotal.inc()
              flushPhase2aBuffer(thrifty = true)
            }

          case FastRound =>
            // If we're in a fast round, and the client knows we're in a fast
            // round (because request.round == round), then the client should
            // not be sending the leader any requests. It only does so if there
            // was a failure. In this case, we change to a higher round.
            leaderLogger.debug("Something went very wrong, SPaxos only works with classic rounds")
        }
    }
  }

  def flushPhase2aBuffer(thrifty: Boolean): Unit = {
    state match {
      case Phase2(_, _, _, phase2aBuffer, phase2aBufferFlushTimer, _, _) =>
        if (phase2aBuffer.size > 0) {
          val msg =
            AcceptorInbound().withPhase2ABuffer(Phase2aBuffer(phase2aBuffer))
          if (thrifty) {
            thriftyAcceptors(quorumSize(round)).foreach(_.send(msg))
          } else {
            acceptors.foreach(_.send(msg))
          }
          phase2aBuffer.clear()
          phase2aBufferFlushTimer.reset()
        } else {
          phase2aBufferFlushTimer.reset()
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

  private def processPhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    def toValueChosen(slot: Slot, entry: Entry): ValueChosen = {
      entry match {
        case ECommand(command) =>
          ValueChosen(slot = slot).withUniqueId(command)
        case ENoop => ValueChosen(slot = slot).withNoop(Noop())
      }
    }

    state match {
      case phase2 @ Phase2(pendingEntries,
      phase2bs,
      _,
      _,
      _,
      valueChosenBuffer,
      valueChosenBufferFlushTimer) =>
        def choose(entry: Entry): Unit = {
          //log(phase2b.slot) = entry
          pendingEntries -= phase2b.slot
          phase2bs -= phase2b.slot

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
        if (valueChosenBuffer.contains(ValueChosen(phase2b.slot))) {
          // Without thriftiness, this is the normal case, so it prints a LOT.
          // So, we comment it out.
          //
          leaderLogger.debug("Value already chosen"
          )
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
            leaderLogger.debug("SPaxos does not support fast rounds")
            metrics.stuckTotal.inc()
        }
    }
  }

  def flushValueChosenBuffer(): Unit = {
    state match {
      case Phase2(_, _, _, _, _, buffer, bufferFlushTimer) =>
        if (buffer.size > 0) {
          leaders.foreach(_.send(LeaderInbound().withValueChosenBuffer(ValueChosenBuffer(buffer))))
          buffer.clear()
          bufferFlushTimer.reset()
        } else {
          bufferFlushTimer.reset()
        }
    }
  }


  // Methods ///////////////////////////////////////////////////////////////////
  def quorumSize(round: Round): Int = {
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

  def resendPhase2as(): Unit = {
    state match {
      case Phase2(pendingEntries, phase2bs, _, phase2aBuffer, _, _, _) =>
        for (slot <- phase2bs.keys) {
          val entryToPhase2a: Entry => Phase2a = {
            case ECommand(command) =>
              Phase2a(slot = slot, round = round).withUniqueId(command)
            case ENoop =>
              Phase2a(slot = slot, round = round).withNoop(Noop())
          }

          (pendingEntries.get(slot), phase2bs.get(slot)) match {
            case (Some(entry), _) =>
              // If we have some pending entry, then we propose that.
              phase2aBuffer.append(entryToPhase2a(entry))

            case (None, Some(phase2bsInSlot)) =>
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

            case (None, None) =>
              // If there is no pending entry and no votes, we propose Noop.
              phase2aBuffer += entryToPhase2a(ENoop)
          }
        }
        flushPhase2aBuffer(thrifty = false)
    }
  }

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
  }
}

