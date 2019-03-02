package frankenpaxos.fastmultipaxos

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.scalajs.js.annotation._

@JSExportAll
object AcceptorInboundSerializer extends ProtoSerializer[AcceptorInbound] {
  type A = AcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Acceptor {
  val serializer = AcceptorInboundSerializer
}

@JSExportAll
class Acceptor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = AcceptorInbound
  override val serializer = AcceptorInboundSerializer

  // Sanity check the Paxos configuration and compute the acceptor's id.
  logger.check(config.acceptorAddresses.contains(address))
  private val acceptorId = config.acceptorAddresses.indexOf(address)

  // The largest round in which this acceptor has received a message. Note that
  // we perform the common MultiPaxos optimization in which every acceptor has
  // a single round for every slot. Also note that `config.roundSystem` can be
  // used to convert the round into a leader.
  @JSExport
  protected var round: Int = -1

  // Channels to the leaders.
  private val leaders: Map[Int, Chan[Leader[Transport]]] = {
    for ((leaderAddress, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](leaderAddress, Leader.serializer)
  }.toMap

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

  // If this acceptor receives a propose request from a client, and
  @JSExport
  protected var nextSlot = 0;

  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import AcceptorInbound.Request
    inbound.request match {
      case Request.ProposeRequest(r) => handleProposeRequest(src, r)
      case Request.Phase1A(r)        => handlePhase1a(src, r)
      case Request.Phase2A(r)        => handlePhase2a(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  private def handleProposeRequest(
      src: Transport#Address,
      proposeRequest: ProposeRequest
  ): Unit = {
    log.get(nextSlot) match {
      case Some(Entry(voteRound, _, Some(r))) if r == round && voteRound < r =>
        // If we previously received the distinguished "any" value in this
        // round and we have not already voted in this round, then we are free
        // to vote for the client's request.
        log.put(nextSlot, Entry(r, VVCommand(proposeRequest.command), None))
        val leader = leaders(config.roundSystem.leader(round))
        leader.send(
          LeaderInbound().withPhase2B(
            Phase2b(acceptorId = acceptorId, slot = nextSlot, round = round)
              .withCommand(proposeRequest.command)
          )
        )
        nextSlot += 1
      case Some(_) | None =>
      // If we have not received the distinguished "any" value, then we
      // simply ignore the client's request.
      //
      // TODO(mwhittaker): Inform the client of the round, so that they can
      // send it to the leader.
    }
  }

  private def handlePhase1a(src: Transport#Address, phase1a: Phase1a): Unit = {
    // Ignore messages from previous rounds.
    if (phase1a.round <= round) {
      logger.info(
        s"An acceptor received a phase 1a message for round " +
          s"${phase1a.round} but is in round $round."
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
          Some(Phase1bVote(slot = s, voteRound = vr).withCommand(command))
        case (s, Entry(vr, VVNoop, _)) =>
          Some(Phase1bVote(slot = s, voteRound = vr).withNoop(Noop()))
        case (_, Entry(_, VVNothing, _)) =>
          None
      })
    val leader = leaders(config.roundSystem.leader(round))
    leader.send(
      LeaderInbound().withPhase1B(
        Phase1b(acceptorId = acceptorId, round = round, vote = votes.to[Seq])
      )
    )
  }

  private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
    val Entry(voteRound, voteValue, anyRound) = log.get(phase2a.slot) match {
      case Some(vote) => vote
      case None       => Entry(-1, VVNothing, None)
    }

    // Ignore messages from smaller rounds.
    if (phase2a.round < round) {
      logger.info(
        s"An acceptor received a phase 2a message for round " +
          s"${phase2a.round} but is in round $round."
      )
      return
    }

    // Ignore messages from our current round if we've already voted.
    // TODO(mwhittaker): For liveness, I think we want to re-send our votes.
    // Otherwise, the leader could be unaware a value is chosen and re-send a
    // phase 2a that is ignored by all the acceptors.
    if (phase2a.round == voteRound) {
      logger.info(
        s"An acceptor received a phase 2a message for round " +
          s"${phase2a.round} but has already voted in round $round."
      )
      return
    }

    // Update our round, vote round, vote value, next slot, and respond to the
    // leader.
    round = phase2a.round
    val leader = leaders(config.roundSystem.leader(round))
    phase2a.value match {
      case Phase2a.Value.Command(command) =>
        log.put(phase2a.slot, Entry(round, VVCommand(command), None))
        leader.send(
          LeaderInbound().withPhase2B(
            Phase2b(acceptorId = acceptorId, slot = phase2a.slot, round = round)
              .withCommand(command)
          )
        )
        if (phase2a.slot >= nextSlot) {
          nextSlot = phase2a.slot + 1
        }

      case Phase2a.Value.Noop(_) =>
        log.put(phase2a.slot, Entry(round, VVNoop, None))
        leader.send(
          LeaderInbound().withPhase2B(
            Phase2b(acceptorId = acceptorId, slot = phase2a.slot, round = round)
              .withNoop(Noop())
          )
        )
        if (phase2a.slot >= nextSlot) {
          nextSlot = phase2a.slot + 1
        }

      case Phase2a.Value.Any(_) =>
        log.put(phase2a.slot, Entry(voteRound, voteValue, Some(round)))

      case Phase2a.Value.AnySuffix(_) =>
        val updatedVotes =
          log
            .prefix()
            .iteratorFrom(phase2a.slot)
            .map({
              case (s, Entry(vr, vv, _)) => (s, Entry(vr, vv, Some(round)))
            })
        for ((slot, entry) <- updatedVotes) {
          log.put(slot, entry)
        }
        log.putTail(log.prefix.lastKey + 1, Entry(-1, VVNothing, Some(round)))

      case Phase2a.Value.Empty => logger.fatal("Empty Phase2a value.")
    }
  }
}
