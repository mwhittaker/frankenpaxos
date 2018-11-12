package zeno.examples

import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer
import zeno.TypedActorClient

@JSExportAll
object PaxosAcceptorInboundSerializer
    extends ProtoSerializer[PaxosAcceptorInbound] {
  type A = PaxosAcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object PaxosAcceptorActor {
  val serializer = PaxosAcceptorInboundSerializer
}

@JSExportAll
class PaxosAcceptorActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: PaxosConfig[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = PaxosAcceptorInbound
  override def serializer = PaxosAcceptorActor.serializer
  override def receive(
      src: Transport#Address,
      msg: PaxosAcceptorInbound
  ): Unit = ???
}
