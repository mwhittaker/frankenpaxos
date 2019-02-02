package zeno.examples

import scala.collection.mutable
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer
import zeno.TypedActorClient

@JSExportAll
object MultiPaxosReplicaInboundSerializer
extends ProtoSerializer[MultiPaxosReplicaInbound] {
  type A = MultiPaxosReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object MultiPaxosReplicaActor {
  val serializer = MultiPaxosReplicaInboundSerializer
}

class MultiPaxosReplicaActor[Transport <: zeno.Transport[Transport]](
  address: Transport#Address,
  transport: Transport,
  logger: Logger,
  config: PaxosConfig[Transport]
  ) extends Actor(address, transport, logger) {
    override type InboundMessage = MultiPaxosReplicaInbound
    override def serializer = MultiPaxosReplicaActor.serializer

    // Verify the Paxos config and get our proposer index.
    logger.check(config.valid())
    logger.check(config.proposerAddresses.contains(address))
    private val index: Int = config.proposerAddresses.indexOf(address)

    // The current state of the replica's state machine
    private var state: String = ""

    // The index of the first slot that has no proposal for it
    private var slotIn: Int = 0;

    // The index of the next slot that needs to be decided before the state machine can be updated
    private var slotOut: Int = 0;

    // Requests received from clients that have not been proposed yet (initially empty)
    private var requests: Set[String] = Set()

    // A set of outstanding proposals (initially empty)
    private var proposals: Set[ProposedValue] = Set()

    // A set of decided proposals (initially empty)
    private var decisions: Set[ProposedValue] = Set()

    // The Multi-Paxos leader(s) to forward proposals to
    private var leaders: Set[Int] = Set()

    // Connections to the acceptors.
    private val acceptors
    : Seq[TypedActorClient[Transport, MultiPaxosAcceptorActor[Transport]]] =
      for (acceptorAddress <- config.acceptorAddresses)
        yield
        typedActorClient[MultiPaxosAcceptorActor[Transport]](
          acceptorAddress,
          MultiPaxosAcceptorActor.serializer
        )

        // A list of the clients awaiting a response.
        private val clients: mutable.Buffer[
        TypedActorClient[Transport, PaxosClientActor[Transport]]
        ] = mutable.Buffer()

        override def receive(
          src: Transport#Address,
          inbound: MultiPaxosReplicaInbound
        ): Unit = {
          import MultiPaxosReplicaInbound.Request
          inbound.request match {
            case Request.ClientRequest(r) => handleClientRequest(src, r)
            case Request.Decision(r)        => handleDecision(src, r)
            case Request.Empty => {
              logger.fatal("Empty PaxosProposerInbound encountered.")
            }
          }
        }

        private def handleClientRequest(src: Transport#Address, request: ClientRequest): Unit = {
        }

        private def handleDecision(src: Transport#Address, request: Decision): Unit = {
        }

  }
