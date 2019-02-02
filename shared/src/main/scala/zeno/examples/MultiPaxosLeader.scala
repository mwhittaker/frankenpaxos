package zeno.examples

import scala.collection.mutable
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer
import zeno.TypedActorClient

@JSExportAll
object MultiPaxosLeaderInboundSerializer
extends ProtoSerializer[MultiPaxosLeaderInbound] {
  type A = MultiPaxosLeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object MultiPaxosLeaderActor {
  val serializer = MultiPaxosLeaderInboundSerializer
}

class MultiPaxosLeaderActor[Transport <: zeno.Transport[Transport]](
  address: Transport#Address,
  transport: Transport,
  logger: Logger,
  config: PaxosConfig[Transport]
  ) extends Actor(address, transport, logger) {
    override type InboundMessage = MultiPaxosLeaderInbound
    override def serializer = MultiPaxosLeaderActor.serializer

    // Verify the Paxos config and get our proposer index.
    logger.check(config.valid())
    logger.check(config.proposerAddresses.contains(address))
    private val index: Int = config.proposerAddresses.indexOf(address)

    // Monotonically increasing (equivalent to round)
    private var ballotNumber: Double = 0

    // Whether the leader is active
    private var active: Boolean = false

    // A mapping between slot number to proposed command for that slot
    private var proposals: Map[Int, String] = Map()

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

        // The set of phase 1b and phase 2b responses from the current ballot
        private var phase1bResponses: mutable.HashSet[Phase1b] = mutable.HashSet()
        private var phase2bResponses: mutable.HashSet[Phase2b] = mutable.HashSet()

        override def receive(
          src: Transport#Address,
          inbound: MultiPaxosLeaderInbound
        ): Unit = {
          import MultiPaxosLeaderInbound.Request
          inbound.request match {
            case Request.ProposeToLeader(r)         => handleProposeRequest(src, r)
            case Request.Adopted(r)                 => handleAdopted(src, r)
            case Request.Preempted(r)               => handlePreempted(src, r)
            case Request.MultiPaxosPhase1B(r)        => handlePhase1b(src, r)
            case Request.MultiPaxosPhase2B(r)        => handlePhase2b(src, r)
            case Request.Empty => {
              logger.fatal("Empty MultiPaxosLeaderInbound encountered.")
            }
          }
        }

      private def handleProposeRequest(src: Transport#Address, request: ProposeToLeader): Unit = {
      }

      private def handleAdopted(src: Transport#Address, request: Adopted): Unit = {
      }

      private def handlePreempted(src: Transport#Address, request: Preempted): Unit = {
      }

      private def handlePhase1b(src: Transport#Address, request: MultiPaxosPhase1b): Unit = {
      }

      private def handlePhase2b(src: Transport#Address, request: MultiPaxosPhase2b): Unit = {
      }
  }
