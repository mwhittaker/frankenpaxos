package frankenpaxos.fastmultipaxos

import collection.mutable
import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.scalajs.js.annotation._

// TODO(mwhittaker): Add client map, and also avoid re-executing the same
// command multiple times.

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
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    electionAddress: Transport#Address,
    heartbeatAddress: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override val serializer = LeaderInboundSerializer

  type AcceptorId = Int
  type Round = Int
  type Slot = Int

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the leader's id.
  logger.check(config.leaderAddresses.contains(address))
  private val leaderId = config.leaderAddresses.indexOf(address)

  // Channels to all other leaders.
  private val otherLeaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses if a != address)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Channels to all the acceptors.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (address <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](address, Acceptor.serializer)

  // The current round. Initially, the leader that owns round 0 is the active
  // leader, and all other leaders are inactive.
  @JSExport
  protected var round: Round =
    if (config.roundSystem.leader(0) == leaderId) 0 else -1

  // The log of chosen commands.
  sealed trait Entry
  case class ECommand(command: Command) extends Entry
  case object ENoop extends Entry

  @JSExport
  protected val log: mutable.SortedMap[Slot, Entry] = mutable.SortedMap()

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
  sealed trait State

  // This leader is not the active leader.
  //
  // TODO(mwhittaker): Keep track of who the active leader is.
  case object Inactive extends State

  // This leader is executing phase 1.
  case class Phase1(
      // Phase 1b responses, indexed by acceptor id.
      phase1bs: mutable.Map[AcceptorId, Phase1b]
  ) extends State

  // This leader has finished executing phase 1 and is now executing phase 2.
  case class Phase2(
      phase2bs: mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]]
      // TODO(mwhittaker): Add timer to resend phase 2a's.
  ) extends State

  @JSExport
  protected var state: State =
    if (round == 0) {
      for (acceptor <- acceptors) {
        acceptor.send(
          AcceptorInbound().withPhase1A(
            Phase1a(round = round, chosenWatermark = chosenWatermark)
          )
        )
      }
      Phase1(mutable.Map())
    } else {
      Inactive
    }

  // Leaders participate in a leader election protocol to maintain a
  // (hopefully) stable leader.
  //
  // TODO(mwhittaker): Pass in leader election options.
  private val election: frankenpaxos.election.Participant[Transport] =
    new frankenpaxos.election.Participant[Transport](
      electionAddress,
      transport,
      logger,
      config.leaderAddresses.to[Set],
      leader = Some(config.leaderAddresses(config.roundSystem.leader(0)))
    )
  // TODO(mwhittaker): Is this thread safe? It's possible that the election
  // participant invokes the callback before this leader has finished
  // initializing?
  election.register(leaderChange)

  // Leaders monitor acceptors to make sure they are still alive.
  //
  // TODO(mwhittaker): Pass in hearbeat options.
  private val heartbeat: frankenpaxos.heartbeat.Participant[Transport] =
    new frankenpaxos.heartbeat.Participant[Transport](
      heartbeatAddress,
      transport,
      logger,
      config.acceptorAddresses.to[Set]
    )

  // Methods ///////////////////////////////////////////////////////////////////
  def leaderChange(leader: Transport#Address): Unit = ???

  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.ProposeRequest(r) => handleProposeRequest(src, r)
      case Request.Phase1B(r)        => handlePhase1b(src, r)
      case Request.Phase2B(r)        => handlePhase2b(src, r)
      case Request.Empty =>
        logger.fatal("Empty LeaderInbound encountered.")
    }
  }

  private def handleProposeRequest(
      src: Transport#Address,
      request: ProposeRequest
  ): Unit = {
    ???
  }

  private def handlePhase1b(
      src: Transport#Address,
      request: Phase1b
  ): Unit = {
    ???
  }

  private def handlePhase2b(
      src: Transport#Address,
      request: Phase2b
  ): Unit = {
    ???
  }
}
