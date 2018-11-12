package zeno.examples

import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer

@JSExportAll
object PaxosProposerInboundSerializer
    extends ProtoSerializer[PaxosProposerInbound] {
  type A = PaxosProposerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object PaxosProposerActor {
  val serializer = PaxosProposerInboundSerializer
}

class PaxosProposerActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = PaxosProposerInbound
  override def serializer = PaxosProposerActor.serializer

  override def receive(
      src: Transport#Address,
      inbound: PaxosProposerInbound
  ): Unit = { ??? }
}
