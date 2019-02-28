package frankenpaxos.fastmultipaxos

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.fastpaxos.Config
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
  // a single round for every slot.
  @JSExport
  protected var round: Int = -1

  // Channels to the leaders.
  private val leaders: Map[Int, Chan[Leader[Transport]]] = {
    for ((leaderAddress, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](leaderAddress, Leader.serializer)
  }.toMap

  // The id of the actor that this acceptor believes is the leader. When a
  // leader initiates phase 1 of the protocol, it includes its id so that
  // acceptors know the current leader.
  @JSExport
  protected var leaderId: Int = 0

  // In Fast Paxos, every acceptor has a single vote round and vote value. With
  // Fast MultiPaxos, we have one pair of vote round and vote value per slot.
  // We call such a pair a vote. The vote also includes the highest round in
  // which an acceptor has received a distinguished "any" value.
  @JSExportAll
  case class Vote(
      voteRound: Int,
      voteValue: Option[String],
      anyRound: Option[Int]
  )

  // The log of votes.
  @JSExport
  protected val log: Log[Vote] = new Log()

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
      case Some(Vote(voteRound, _, Some(r))) if r == round && voteRound < r =>
        // If we previously received the distinguished "any" value in this
        // round and we have not already voted in this round, then we are free
        // to vote for the client's request.
        log.put(nextSlot, Vote(r, Some(proposeRequest.v), None))
        nextSlot += 1
        leaders(leaderId).send(
          LeaderInbound().withPhase2B(
            Phase2b(
              acceptorId = acceptorId,
              slot = nextSlot,
              round = round,
              command = proposeRequest.v,
              clientAddress = Some(
                ByteString.copyFrom(transport.addressSerializer.toBytes(src))
              )
            )
          )
        )
      case Some(_) | None =>
      // If we have not received the distinguished "any" value, then we
      // simply ignore the client's request.
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
    // exclude votes below the chosen watermark. We also make sure not to
    // return votes for slots that we haven't actually voted in.
    round = phase1a.round
    leaderId = phase1a.leaderId
    val votes = log
      .prefix()
      .iteratorFrom(phase1a.chosenWatermark + 1)
      .flatMap({
        case (slot, Vote(voteRound, Some(v), _)) =>
          Some(AcceptorVote(slot = slot, voteRound = voteRound, voteValue = v))
        case _ => None
      })
    leaders(leaderId).send(
      LeaderInbound().withPhase1B(
        Phase1b(acceptorId = acceptorId, round = round, vote = votes.to[Seq])
      )
    )
  }

  private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
    val Vote(voteRound, voteValue, anyRound) = log.get(phase2a.slot) match {
      case Some(vote) => vote
      case None       => Vote(-1, None, None)
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
    if (phase2a.round == voteRound) {
      logger.info(
        s"An acceptor received a phase 2a message for round " +
          s"${phase2a.round} but has already voted in round $round."
      )
      return
    }

    // Update our round, vote round, vote value, and respond to the leader.
    round = phase2a.round
    leaderId = phase2a.leaderId
    phase2a.value match {
      case Some(v) =>
        log.put(phase2a.slot, Vote(round, Some(v), None))
        leaders(leaderId).send(
          LeaderInbound().withPhase2B(
            Phase2b(acceptorId = acceptorId,
                    slot = phase2a.slot,
                    round = round,
                    command = v)
          )
        )
      case None =>
        log.put(phase2a.slot, Vote(voteRound, voteValue, Some(round)))
    }
  }
}
