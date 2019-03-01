package frankenpaxos.fastmultipaxos

import collection.immutable.SortedMap
import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
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

  // The client table records the response to the latest request from each
  // client. For example, if command c1 sends command x with id 2 to a leader
  // and the leader later executes x yielding response y, then c1 maps to (2,
  // y) in the client table.
  @JSExport
  protected val clientTable: mutable.Map[Transport#Address, (Int, String)] =
    mutable.Map()

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
      phase1bs: mutable.Map[AcceptorId, Phase1b],
      // Pending proposals.
      pendingProposals: mutable.Buffer[ProposeRequest],
      // A timer to resend phase1bs.
      resendPhase1as: Transport#Timer
  ) extends State

  private val resendPhase1asTimer: Transport#Timer = timer(
    "resendPhase1as",
    // TODO(mwhittaker): Pass in as parameter.
    java.time.Duration.ofSeconds(3),
    sendPhase1as
  )

  // This leader has finished executing phase 1 and is now executing phase 2.
  case class Phase2(
      // For each slot, the entry that is waiting to get chosen in that slot.
      pendingEntries: mutable.SortedMap[Slot, Entry],
      // For each slot, the set of phase 2b messages for that slot.
      phase2bs: mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]],
      // A timer to resend all pending phase 2a messages.
      resendPhase2as: Transport#Timer
  ) extends State

  private val resendPhase2asTimer: Transport#Timer = timer(
    "resendPhase2as",
    // TODO(mwhittaker): Pass in as parameter.
    java.time.Duration.ofSeconds(3),
    // TODO(mwhittaker): Implement.
    ???
  )

  @JSExport
  protected var state: State =
    if (round == 0) {
      sendPhase1as()
      Phase1(mutable.Map(), mutable.Buffer(), resendPhase1asTimer)
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
  private def sendPhase1as(): Unit = {
    for (acceptor <- acceptors) {
      acceptor.send(
        AcceptorInbound().withPhase1A(
          Phase1a(round = round, chosenWatermark = chosenWatermark)
        )
      )
    }
  }

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
    // If we've cached the result of this proposed command in the client table,
    // then we can reply to the client directly. Note that only the leader
    // replies to the client since ProposeReplies include the round of the
    // leader, and only the leader knows this.
    clientTable.get(src) match {
      case Some((clientId, result)) =>
        if (request.command.clientId == clientId && state != Inactive) {
          val client = chan[Client[Transport]](src, Client.serializer)
          client.send(
            ClientInbound().withProposeReply(
              ProposeReply(round = round, clientId = clientId, result = result)
            )
          )
        }
        return
      case None =>
    }

    state match {
      case Inactive =>
        logger.debug("Leader received propose request but is inactive.")

      case Phase1(_, pendingProposals, _) =>
        // We buffer all pending proposals in phase 1 and process them later
        // when we enter phase 2.
        pendingProposals += request

      case Phase2(pendingEntries, phase2bs, _) =>
        for (acceptor <- acceptors) {
          acceptor.send(
            AcceptorInbound().withPhase2A(
              Phase2a(slot = nextSlot, round = round)
                .withCommand(request.command)
            )
          )
        }
        pendingEntries(nextSlot) = ECommand(request.command)
        phase2bs(nextSlot) = mutable.Map()
        nextSlot += 1
    }
  }

  // Given a quorum of phase1b votes, determine a safe value to propose in slot
  // `slot`.
  def chooseProposal(
      votes: collection.Map[AcceptorId, SortedMap[Slot, Phase1bVote]],
      slot: Slot
  ): Entry = {
    ???
  }

  private def handlePhase1b(
      src: Transport#Address,
      request: Phase1b
  ): Unit = {
    state match {
      case Inactive | Phase2(_, _, _) =>
        logger.debug("Leader received phase 1b, but is not in phase 1.")

      case Phase1(phase1bs, pendingProposals, resendPhase1as) =>
        if (request.round != round) {
          logger.debug(s"""Leader received phase 1b in round ${request.round},
            |but is in round $round.""".stripMargin)
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
          votes.map({ case (a, vs) => vs.firstKey }).max,
          log.lastKey
        )

        // For every unchosen slot between the chosenWatermark and endSlot,
        // choose a value to propose and propose it.
        val pendingEntries = mutable.SortedMap[Slot, Entry]()
        val phase2bs =
          mutable.SortedMap[Slot, mutable.Map[AcceptorId, Phase2b]]()
        for (slot <- chosenWatermark to endSlot) {
          val proposal: Entry = chooseProposal(votes, slot)
          val phase2a = Phase2a(slot = slot, round = round)
          val msg = proposal match {
            case ECommand(command) =>
              AcceptorInbound().withPhase2A(phase2a.withCommand(command))
            case ENoop =>
              AcceptorInbound().withPhase2A(phase2a.withNoop(Noop()))
          }

          pendingEntries(slot) = proposal
          phase2bs(slot) = mutable.Map[AcceptorId, Phase2b]()
          acceptors.foreach(_.send(msg))
        }

        state = Phase2(pendingEntries, phase2bs, ???)
        // {
        //   for ((acceptorId, phase1b) <- phase1bs) yield {
        //   }
        // }.toMap

        // Convert every phase1b vote into a sorted map.
        // figure out the highest and lowest slots
        // iterate over every slot, running phase 2
        //   get highest round
        //   get values in that round
        //   if -1: noop
        //   if single value: propose
        //   if majority of majority: propose
        //   else noop
        // if fast round, update any any suffix
        //
        // start resendPhase2as
        //
        // implement resendPhase2as
        //   - will need to cache requests
        //   - for every pending guy, resend phase2a

        for (proposal <- pendingProposals) {
          // TODO(mwhittaker): Call handleProposeRequest.
          ???
        }
    }
  }

  private def handlePhase2b(
      src: Transport#Address,
      request: Phase2b
  ): Unit = {
    ???
  }
}
