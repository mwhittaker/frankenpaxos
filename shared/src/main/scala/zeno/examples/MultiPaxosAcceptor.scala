package zeno.examples

import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer
import zeno.TypedActorClient

@JSExportAll
object MultiPaxosAcceptorInboundSerializer
extends ProtoSerializer[MultiPaxosAcceptorInbound] {
  type A = MultiPaxosAcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object MultiPaxosAcceptorActor {
  val serializer = MultiPaxosAcceptorInboundSerializer
}

@JSExportAll
class MultiPaxosAcceptorActor[Transport <: zeno.Transport[Transport]](
  address: Transport#Address,
  transport: Transport,
  logger: Logger,
  config: MultiPaxosConfig[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = MultiPaxosAcceptorInbound
  override def serializer = MultiPaxosAcceptorActor.serializer

  // Sanity check the Paxos configuration and retrieve acceptor index.
  logger.check(config.acceptorAddresses.contains(address))
  private val index = config.acceptorAddresses.indexOf(address)

  // The largest ballot the acceptor has adopted
  var ballotNumber: Double = -1

  // The set of proposals the acceptor accepted
  var accepted: Set[ProposedValue] = Set()

  override def receive(
    src: Transport#Address,
    inbound: MultiPaxosAcceptorInbound
  ): Unit = {
    import MultiPaxosAcceptorInbound.Request
    inbound.request match {
      case Request.Phase1A(r) => handlePhase1a(src, r)
      case Request.Phase2A(r) => handlePhase2a(src, r)
      case Request.Empty => {
        logger.fatal("Empty MultiPaxosAcceptorInbound encountered.")
      }
    }
  }

  private def handlePhase1a(src: Transport#Address, phase1a: MultiPaxosPhase1a): Unit = {
    if (phase1a.ballot > ballotNumber) {
      ballotNumber = phase1a.ballot
    }
    // send phase 1b message
    val leader = typedActorClient[MultiPaxosLeaderActor[Transport]](
      src,
      MultiPaxosLeaderActor.serializer
    )
    leader.send(
      MultiPaxosLeaderInbound().withPhase1B(
        MultiPaxosPhase1b(ballot = ballotNumber, proposals = accepted.toSeq)
      )
    )
  }

  private def handlePhase2a(src: Transport#Address, phase2a: MultiPaxosPhase2a): Unit = {
    if (phase2a.proposal.ballot == ballotNumber) {
      accepted += ProposedValue(ballotNumber, phase2a.proposal.slot, phase2a.proposal.command)
    }

    val leader = typedActorClient[MultiPaxosLeaderActor[Transport]](
      src,
      MultiPaxosLeaderActor.serializer
    )

    //println("Phase 2a proposal: " + phase2a.proposal.command + ", " + phase2a.proposal.slot)
    var temp: ProposedValue = ProposedValue(phase2a.proposal.ballot, phase2a.proposal.slot, phase2a.proposal.command)
    leader.send(
      MultiPaxosLeaderInbound().withPhase2B(
        MultiPaxosPhase2b(
          acceptorId = index,
          ballot = ballotNumber,
          phase2AProposal = temp
        )
      )
    )
  }
}
