package frankenpaxos.multipaxos

import scala.collection.mutable
import scala.scalajs.js.annotation._
import frankenpaxos._

@JSExportAll
object ReplicaInboundSerializer extends ProtoSerializer[ReplicaInbound] {
  type A = ReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer
}

case class ClientProposal(slot: Integer, command: String)

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer

  // Verify the Paxos config and get our proposer index.
  logger.check(config.valid())
  //logger.check(config.proposerAddresses.contains(address))
  //private val index: Int = config.proposerAddresses.indexOf(address)

  // The current state of the replica's state machine
  // TODO(michael): Introduce a state machine abstraction.
  @JSExport
  protected var state: String = ""

  // The index of the first slot that has no proposal for it
  @JSExport
  protected var slotIn: Int = 0

  // The index of the next slot that needs to be decided before the state machine can be updated
  @JSExport
  protected var slotOut: Int = 0

  // Requests received from clients that have not been proposed yet (initially empty)
  // TODO(neil): In the long term, I think clients should annotate commands
  // with a unique client id. Implement this later, though. -Michael.
  @JSExport
  protected var requests: Set[String] = Set()

  // A set of outstanding proposals (initially empty)
  // TODO(neil): Would it make more sense to have a Map from slot to command
  // instead of a set of tuples? -Michael.
  @JSExport
  protected var proposals: Set[ClientProposal] = Set()

  // A set of decided proposals (initially empty)
  // TODO(neil): Would it make more sense to have a Map from slot to command
  // instead of a set of tuples? -Michael.
  @JSExport
  protected var decisions: Set[ClientProposal] = Set()

  // Delay constant to keep processing commands
  private val WINDOW: Int = 3

  // The Multi-Paxos leader(s) to forward proposals to
  //private var leaders: Set[Int] = Set()
  private var leaders: Seq[TypedActorClient[Transport, Leader[Transport]]] =
    for (leaderAddress <- config.leaderAddresses)
      yield
        typedActorClient[Leader[Transport]](
          leaderAddress,
          Leader.serializer
        )

  // Connections to the acceptors.
  private val acceptors: Seq[TypedActorClient[Transport, Acceptor[Transport]]] =
    for (acceptorAddress <- config.acceptorAddresses)
      yield
        typedActorClient[Acceptor[Transport]](
          acceptorAddress,
          Acceptor.serializer
        )

  // A list of the clients awaiting a response.
  private val clients: mutable.Buffer[
    TypedActorClient[Transport, Client[Transport]]
  ] = mutable.Buffer()

  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    import ReplicaInbound.Request
    inbound.request match {
      case Request.ClientRequest(r) => handleClientRequest(src, r)
      case Request.Decision(r)      => handleDecision(src, r)
      case Request.Empty => {
        logger.fatal("Empty PaxosProposerInbound encountered.")
      }
    }
    propose()
  }

  private def isCommandDecided(command: String): Boolean = {
    for (decision <- decisions) {
      if (decision.slot == slotIn)
        return true
    }
    false
  }

  private def existsSlotOutCommandDecision(): Option[String] = {
    for (decision <- decisions) {
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

  private def propose(): Unit = {
    while (slotIn < slotOut + WINDOW && requests.nonEmpty) {
      var command: String = requests.head
      if (!isCommandDecided(command)) {
        requests -= command
        proposals += ClientProposal(slot = slotIn, command = command)
        for (leader <- leaders) {
          //println("Command being sent to leader")
          leader.send(
            LeaderInbound().withProposeRequest(
              ProposeToLeader(slot = slotIn, command = command)
            )
          )
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
      /*clients.head.send(ClientInbound().withProposeResponse(
              ProposeResponse(response = state)
            ))*/
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      request: ClientRequest
  ): Unit = {
    requests += request.command
  }

  private def handleDecision(
      src: Transport#Address,
      request: Decision
  ): Unit = {
    //println("Decision received by the replica for slot: " + request.slot + " and command: " + request.command)
    decisions += ClientProposal(request.slot, request.command)
    var command: Option[String] = existsSlotOutCommandDecision()
    while (command.isDefined) {
      val commandProposed: Option[String] = existsSlotOutCommandProposal()
      if (commandProposed.isDefined) {
        proposals -= ClientProposal(
          slot = slotOut,
          command = commandProposed.get
        )
        if (command != commandProposed) {
          requests += commandProposed.get
        }
      }
      perform(command.get)
      command = existsSlotOutCommandDecision()
    }
  }

}
