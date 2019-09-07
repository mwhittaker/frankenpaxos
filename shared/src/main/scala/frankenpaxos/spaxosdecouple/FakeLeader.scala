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
case class FakeLeaderOptions(valueChosenMaxBufferSize: Int, thriftySystem: ThriftySystem, heartbeatOptions: HeartbeatOptions) {}

@JSExportAll
object FakeLeaderOptions {
  val default = FakeLeaderOptions(
    valueChosenMaxBufferSize = 100,
    thriftySystem = ThriftySystem.Closest,
    heartbeatOptions = HeartbeatOptions.default
  )
}

@JSExportAll
class FakeLeaderMetrics(collectors: Collectors) {
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


  @JSExport
  protected var state: State = Phase2()

  // In a classic round, leaders receive commands from clients and relay
  // them on to acceptors. pendingEntries stores these commands that are
  // pending votes. Note that during a fast round, a leader may not have a
  // pending command for a slot, even though it does have phase 2bs for it.
  protected var pendingEntries: mutable.SortedMap[Slot, Entry] = mutable.SortedMap[Slot, Entry]()
  // For each slot, the set of phase 2b messages for that slot. In a
  // classic round, all the phase 2b messages will be for the same command.
  // In a fast round, they do not necessarily have to be.
  protected var phase2bs: mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]] = mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]]()

  // A set of buffered value chosen messages.
  protected var valueChosenBuffer: mutable.Buffer[ValueChosen] = mutable.Buffer[ValueChosen]()

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
    round = phase2aBuffer.phase2A.head.round
    flushPhase2aBuffer(phase2aBuffer, thrifty = true)
  }

  def flushPhase2aBuffer(phase2aBuffer: Phase2aBuffer, thrifty: Boolean): Unit = {
    val msg =
      AcceptorInbound().withPhase2ABuffer(phase2aBuffer)
    if (thrifty) {
      thriftyAcceptors(quorumSize(round)).foreach(_.send(msg))
    } else {
      acceptors.foreach(_.send(msg))
    }
  }


  private def handlePhase2bBuffer(
                                   src: Transport#Address,
                                   phase2bBuffer: Phase2bBuffer
                                 ): Unit = {
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

    def choose(entry: Entry): Unit = {
      pendingEntries -= phase2b.slot
      phase2bs -= phase2b.slot

      valueChosenBuffer += toValueChosen(phase2b.slot, entry)
      if (valueChosenBuffer.size >= options.valueChosenMaxBufferSize) {
        for (leader <- leaders) {
          leader.send(LeaderInbound().withValueChosenBuffer(ValueChosenBuffer(valueChosenBuffer)))
        }
        valueChosenBuffer.clear()
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

    // Wait for sufficiently many phase2b replies.
    phase2bs.getOrElseUpdate(phase2b.slot, mutable.Map())
    phase2bs(phase2b.slot).put(phase2b.acceptorId, phase2b)
    phase2bChosenInSlot(phase2, phase2b.slot) match {
      case NothingReadyYet =>
      // Don't do anything.

      case ClassicReady(entry) =>
        choose(entry)

      case FastReady(entry) =>
        choose(entry)

      case FastStuck =>
        // The fast round is stuck, so we start again in a higher round.
        // TODO(mwhittaker): We might want to have all stuck things pending
        // for the next round.
        leaderLogger.debug(
          s"Slot ${phase2b.slot} is stuck. Changing to a higher round."
        )
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

