package zeno.examples

import scala.collection.mutable
import scala.scalajs.js.annotation._
import zeno._

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

case class ClientProposal(slot: Integer, command: String)
class MultiPaxosReplicaActor[Transport <: zeno.Transport[Transport]](
  address: Transport#Address,
  transport: Transport,
  logger: Logger,
  config: MultiPaxosConfig[Transport]
  ) extends Actor(address, transport, logger) {
    override type InboundMessage = MultiPaxosReplicaInbound
    override def serializer = MultiPaxosReplicaActor.serializer

    // Verify the Paxos config and get our proposer index.
    logger.check(config.valid())
    //logger.check(config.proposerAddresses.contains(address))
    //private val index: Int = config.proposerAddresses.indexOf(address)

    case class RequestClient(command: String, clientAddress: Transport#Address)

    // The current state of the replica's state machine
    private var state: String = ""

    // The index of the first slot that has no proposal for it
    private var slotIn: Int = 0

    // The index of the next slot that needs to be decided before the state machine can be updated
    private var slotOut: Int = 0

    // Requests received from clients that have not been proposed yet (initially empty)
    private var requests: Set[String] = Set()

    // A set of outstanding proposals (initially empty)
    private var proposals: Set[ClientProposal] = Set()

    // A set of decided proposals (initially empty)
    var decisions: Set[ClientProposal] = Set()

    // Delay constant to keep processing commands
    private val WINDOW: Int = 3

    // The Multi-Paxos leader(s) to forward proposals to
    //private var leaders: Set[Int] = Set()
    private var leaders
    : Seq[TypedActorClient[Transport, MultiPaxosLeaderActor[Transport]]] =
      for (leaderAddress <- config.leaderAddresses)
        yield
        typedActorClient[MultiPaxosLeaderActor[Transport]](
          leaderAddress,
          MultiPaxosLeaderActor.serializer
        )



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
            TypedActorClient[Transport, MultiPaxosClientActor[Transport]]
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
              propose()
            }
        private def isCommandDecided(command: String): Boolean = {
          for (decision <- decisions.toIterator) {
            if (decision.slot == slotIn)
              return true
          }
          false
        }

        private def existsSlotOutCommandDecision(): Option[String] = {
          for (decision <- decisions.toIterator) {
            if (decision.slot == slotOut)
              return Some(decision.command)
          }
          None
        }

        private def existsSlotOutCommandProposal(): Option[String] = {
          for (proposal <- proposals.toIterator) {
            if (proposal.slot == slotOut)
              return Some(proposal.command)
          }
          None
        }

        private def propose() : Unit = {
          while (slotIn < slotOut + WINDOW && requests.nonEmpty) {
            var command: String = requests.head
            if (!isCommandDecided(command)) {
              requests -= command
              proposals += ClientProposal(slot = slotIn, command = command)
              for (leader <- leaders) {
                //println("Command being sent to leader")
                leader.send(MultiPaxosLeaderInbound().withProposeRequest(ProposeToLeader(slot = slotIn, command = command)))
              }
            }
            slotIn += 1
          }
        }

        private def readyToPerform(): Boolean = {
          for (decision <- decisions) {
            if (decision.slot < slotOut) {
              return true
            }
          }
          false
        }

        private def perform(command: String): Unit = {
          if (readyToPerform) {
            slotOut += 1
          } else {
            state = state + ", " + command
            slotOut += 1
            /*clients.head.send(MultiPaxosClientInbound().withProposeResponse(
              ProposeResponse(response = state)
            ))*/
          }
        }

        private def handleClientRequest(src: Transport#Address, request: ClientRequest): Unit = {
          requests += request.command
        }

        private def handleDecision(src: Transport#Address, request: Decision): Unit = {
          //println("Decision received by the replica for slot: " + request.slot + " and command: " + request.command)
          decisions += ClientProposal(request.slot, request.command)
          var command: Option[String] = existsSlotOutCommandDecision()
          while (command.isDefined) {
            val commandProposed: Option[String] = existsSlotOutCommandProposal()
            if (commandProposed.isDefined) {
              proposals -= ClientProposal(slot = slotOut, command = commandProposed.get)
              if (command != commandProposed) {
                requests += commandProposed.get
              }
            }
            perform(command.get)
            command = existsSlotOutCommandDecision()
          }
        }

  }
