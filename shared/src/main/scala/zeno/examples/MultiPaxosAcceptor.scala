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

// Stores the ballot number, slot number, and command for a proposal
//case class ProposedValue (ballotNumber: Double, slotNumber: Int, command: String)

@JSExportAll
class MultiPaxosAcceptorActor[Transport <: zeno.Transport[Transport]](
  address: Transport#Address,
  transport: Transport,
  logger: Logger,
  config: PaxosConfig[Transport]
  ) extends Actor(address, transport, logger) {
    override type InboundMessage = PaxosAcceptorInbound
    override def serializer = PaxosAcceptorActor.serializer

    // Sanity check the Paxos configuration and retrieve acceptor index.
    logger.check(config.acceptorAddresses.contains(address))
    private val index = config.acceptorAddresses.indexOf(address)

    // The largest ballot the acceptor has adopted
    var ballotNumber = -1;

    // The set of proposals the acceptor accepted
    var accepted: Set[ProposedValue] = Set()

    override def receive(
      src: Transport#Address,
      inbound: MultiPaxosAcceptorInbound
    ): Unit = {
      import MultiPaxosAcceptorInbound.Request
      inbound.request match {
        case Request.MultiPaxosPhase1A(r) => handlePhase1a(src, r)
        case Request.MultiPaxosPhase2A(r) => handlePhase2a(src, r)
        case Request.Empty => {
          logger.fatal("Empty MultiPaxosAcceptorInbound encountered.")
        }
      }
    }

  private def handlePhase1a(src: Transport#Address, phase1a: MultiPaxosPhase1a): Unit = {
  }

  private def handlePhase2a(src: Transport#Address, phase2a: MultiPaxosPhase2a): Unit = {
  }
  }
