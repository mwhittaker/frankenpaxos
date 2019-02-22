package frankenpaxos.paxos

import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Chan

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
  override def serializer = Acceptor.serializer

  // Sanity check the Paxos configuration and retrieve acceptor index.
  logger.check(config.acceptorAddresses.contains(address))
  private val index = config.acceptorAddresses.indexOf(address)

  // The largest round in which this acceptor has received a message.
  var round: Int = -1;

  // The largest round in which this acceptor has voted.
  var voteRound: Int = -1;

  // The value that this acceptor voted for in voteRound, or None if the
  // acceptor hasn't voted yet.
  var voteValue: Option[String] = None;

  override def receive(
      src: Transport#Address,
      inbound: AcceptorInbound
  ): Unit = {
    import AcceptorInbound.Request
    inbound.request match {
      case Request.Phase1A(r) => handlePhase1a(src, r)
      case Request.Phase2A(r) => handlePhase2a(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
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

    // Bump our round and send the leader our vote round and vote value.
    round = phase1a.round
    val leader =
      chan[Leader[Transport]](src, Leader.serializer)
    leader.send(
      LeaderInbound().withPhase1B(
        Phase1b(
          round = round,
          acceptorId = index,
          voteRound = voteRound
        ).update(_.optionalVoteValue := voteValue)
      )
    )
  }

  private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
    // Ignore messages from smaller rounds.
    if (phase2a.round < round) {
      logger.info(
        s"An acceptor received a phase 2a message for round " +
          s"${phase2a.round} but is in round $round."
      )
      return
    }

    // Ignore messages from our current round if we've already voted.
    if (phase2a.round == round && phase2a.round == voteRound) {
      logger.info(
        s"An acceptor received a phase 2a message for round " +
          s"${phase2a.round} but has already voted in round $round."
      )
      return
    }

    // Update our state and send back an ack to the leader.
    logger.check_ge(phase2a.round, round)
    round = phase2a.round
    voteRound = phase2a.round
    voteValue = Some(phase2a.value)

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    leader.send(
      LeaderInbound().withPhase2B(Phase2b(acceptorId = index, round = round))
    )
  }
}
