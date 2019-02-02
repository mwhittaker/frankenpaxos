package zeno.examples

import scala.collection.mutable.Buffer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer
import zeno.TypedActorClient

@JSExportAll
object MultiPaxosClientInboundSerializer
extends ProtoSerializer[MultiPaxosClientInbound] {
  type A = MultiPaxosClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object MultiPaxosClientActor {
  val serializer = MultiPaxosClientInboundSerializer
}

@JSExportAll
class MultiPaxosClientActor[Transport <: zeno.Transport[Transport]](
  address: Transport#Address,
  transport: Transport,
  logger: Logger,
  config: PaxosConfig[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = MultiPaxosClientInbound
  override def serializer = MultiPaxosClientActor.serializer

  // The set of replicas.
  private val proposers
  : Seq[TypedActorClient[Transport, MultiPaxosReplicaActor[Transport]]] =
    for (proposerAddress <- config.proposerAddresses)
      yield
      typedActorClient[MultiPaxosReplicaActor[Transport]](
        proposerAddress,
        MultiPaxosReplicaActor.serializer
      )

      // valueProposed holds a proposed value, if one has been proposed. Once a
      // Paxos client has proposed a value, it will not propose any other value.
      private var proposedValue: Option[String] = None

      private var chosenValue: Option[String] = None

      // The state returned to client after command was proposed
      var state: String = ""

      // A list of promises to fulfill once a value has been chosen.
      // TODO(mwhittaker): Replace with futures/promises.
      private var promises: Buffer[Promise[String]] = Buffer()

      override def receive(
        src: Transport#Address,
        inbound: MultiPaxosClientInbound
      ): Unit = {
      }

}
