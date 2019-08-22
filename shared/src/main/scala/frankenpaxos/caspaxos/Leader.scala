package frankenpaxos.caspaxos

import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.collection.mutable
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
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.ClientRequest(r) => handleClientRequest(src, r)
      case Request.Phase1B(r)       => handlePhase1b(src, r)
      case Request.Phase2B(r)       => handlePhase2b(src, r)
      case Request.Nack(r)          => handleNack(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }
}
