package frankenpaxos.paxos

import scala.collection.mutable
import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.TypedActorClient

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

class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  // Verify the Paxos config and get our leader index.
  logger.check(config.valid())
  logger.check(config.leaderAddresses.contains(address))
  private val index: Int = config.leaderAddresses.indexOf(address)

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

  // The leader's round number. With n leaders, leader i uses round
  // numbers i, i + n, i + 2n, i + 3n, etc.
  private var round: Int = index

  // The current status of the leader. A leader is either idle, running
  // phase 1, running phase 2, or has learned that a value is chosen.
  object Status extends Enumeration {
    type Status = Value
    val Idle, Phase1, Phase2, Chosen = Value
  }
  import Status._
  private var status: Status = Idle

  // The value currently being proposed in round `round`.
  private var proposedValue: Option[String] = None

  // The set of phase 1b and phase 2b responses from the current round.
  private var phase1bResponses: mutable.HashSet[Phase1b] = mutable.HashSet()
  private var phase2bResponses: mutable.HashSet[Phase2b] = mutable.HashSet()

  // The chosen value.
  var chosenValue: Option[String] = None

  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.ProposeRequest(r) => handleProposeRequest(src, r)
      case Request.Phase1B(r)        => handlePhase1b(src, r)
      case Request.Phase2B(r)        => handlePhase2b(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleProposeRequest(
      src: Transport#Address,
      request: ProposeRequest
  ): Unit = {
    // If we've already chosen a value, we don't have to contact the
    // acceptors. We can return directly to the client.
    chosenValue match {
      case Some(chosen) => {
        logger.check_eq(status, Chosen)
        val client = typedActorClient[Client[Transport]](
          src,
          Client.serializer
        )
        client.send(
          ClientInbound().withProposeReply(ProposeReply(chosen))
        )
        return
      }
      case None => {}
    }

    // Begin a new round with the newly proposed value.
    round += config.n
    proposedValue = Some(request.v)
    status = Phase1
    phase1bResponses.clear()
    phase2bResponses.clear()

    // Send phase 1a messages to all of the acceptors.
    for (acceptor <- acceptors) {
      acceptor.send(
        AcceptorInbound().withPhase1A(Phase1a(round = round))
      )
    }

    // Remember the client. We'll respond back to the client later.
    clients += typedActorClient[Client[Transport]](
      src,
      Client.serializer
    )
  }

  private def handlePhase1b(src: Transport#Address, request: Phase1b): Unit = {
    // If we're not in phase 1, don't respond to phase 1b responses.
    if (status != Phase1) {
      logger.info(
        s"A leader received a phase 1b response but is not in " +
          s"phase 1: ${request}."
      )
      return
    }

    // Ignore responses that are not in our current round.
    if (request.round != round) {
      logger.info(
        s"A leader received a phase 1b response for round " +
          s"${request.round} but is in round ${round}: ${request}."
      )
      return
    }

    logger.check_eq(status, Phase1)
    logger.check_eq(request.round, round)
    phase1bResponses += request

    // Wait until we have a quorum of phase1b messages.
    if (phase1bResponses.size < config.f + 1) {
      return
    }

    // Select the largest vote round k, and the corresponding vote value v. If
    // we decide not to go with our initially proposed value, make sure not to
    // forget to update the proposed value.
    val k = phase1bResponses.maxBy(_.voteRound).voteRound
    val v = {
      if (k == -1) {
        logger.check(proposedValue.isDefined)
        proposedValue.get
      } else {
        val vs =
          phase1bResponses.filter(_.voteRound == k).map(_.voteValue.get)
        logger.check_eq(vs.size, 1)
        vs.iterator.next()
      }
    }
    proposedValue = Some(v)

    // Start phase 2.
    for (acceptor <- acceptors) {
      acceptor.send(
        AcceptorInbound().withPhase2A(Phase2a(round = round, value = v))
      )
    }
    status = Phase2
  }

  private def handlePhase2b(src: Transport#Address, request: Phase2b): Unit = {
    // If we're not in phase 2, don't respond to phase 2b responses.
    if (status != Phase2) {
      logger.info(
        s"A leader received a phase 2b response but is not in " +
          s"phase 2: ${request}."
      )
      return
    }

    // Ignore responses that are not in our current round.
    if (request.round != round) {
      logger.info(
        s"A leader received a phase 2b response for round " +
          s"${request.round} but is in round ${round}: ${request}."
      )
      return
    }

    logger.check_eq(status, Phase2)
    logger.check_eq(request.round, round)
    phase2bResponses += request

    // Wait until we have a quorum of phase2b messages.
    if (phase2bResponses.size < config.f + 1) {
      return
    }

    // At this point, the proposed value is chosen.
    logger.check(proposedValue.isDefined)
    val chosen = proposedValue.get

    // Make sure the chosen value is the same as any previously chosen
    // value.
    chosenValue match {
      case Some(oldChosen) => {
        logger.check_eq(oldChosen, chosen)
      }
      case None => {}
    }
    chosenValue = Some(chosen)
    status = Chosen

    // Reply to the clients.
    for (client <- clients) {
      client.send(
        ClientInbound().withProposeReply(ProposeReply(chosen = chosen))
      )
    }
    clients.clear()
  }
}
