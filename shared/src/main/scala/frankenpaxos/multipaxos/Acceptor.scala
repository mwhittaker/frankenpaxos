package frankenpaxos.multipaxos

import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.TypedActorClient

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

  // The largest ballot the acceptor has adopted
  var ballotNumber: Double = -1

  // The set of proposals the acceptor accepted
  // TODO(neil): Would it make sense to have this be a map from ballot to (vote
  // round, vote value)? -Michael.
  var accepted: Set[ProposedValue] = Set()

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

  private def handlePhase1a(
      src: Transport#Address,
      phase1a: Phase1a
  ): Unit = {
    // TODO(neil): Shouldn't an acceptor ignore a phase1a if the ballot number
    // is less than or equal to the current ballot number? -Michael.
    if (phase1a.ballot > ballotNumber) {
      ballotNumber = phase1a.ballot
    }
    // send phase 1b message
    val leader = typedActorClient[Leader[Transport]](src, Leader.serializer)
    // TODO(michael): Eventually, have a leader tell the acceptor which
    // entries it doesn't know about. This is a perf optimization and can be
    // left for later.
    leader.send(
      LeaderInbound().withPhase1B(
        Phase1b(ballot = ballotNumber, proposals = accepted.toSeq)
      )
    )
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    // TODO(neil): I think an acceptor can accept even if the proposed ballot
    // is larger than the acceptor's ballot.
    if (phase2a.proposal.ballot == ballotNumber) {
      accepted += ProposedValue(
        ballotNumber,
        phase2a.proposal.slot,
        phase2a.proposal.command
      )
    }

    val leader = typedActorClient[Leader[Transport]](src, Leader.serializer)

    //println("Phase 2a proposal: " + phase2a.proposal.command + ", " + phase2a.proposal.slot)
    var temp: ProposedValue = ProposedValue(
      phase2a.proposal.ballot,
      phase2a.proposal.slot,
      phase2a.proposal.command
    )
    // TODO(neil): What's the phase2AProposal for? -Michael.
    leader.send(
      LeaderInbound().withPhase2B(
        Phase2b(
          acceptorId = index,
          ballot = ballotNumber,
          phase2AProposal = temp
        )
      )
    )
  }
}
