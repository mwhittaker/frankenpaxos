package frankenpaxos.fastpaxos

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._

@JSExportAll
object ClientInboundSerializer extends ProtoSerializer[ClientInbound] {
  type A = ClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Client {
  val serializer = ClientInboundSerializer
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = ClientInbound
  override val serializer = ClientInboundSerializer

  // Leader channels.
  private val leaders: Seq[Chan[Transport, Leader[Transport]]] =
    for (leaderAddress <- config.leaderAddresses)
      yield chan[Leader[Transport]](leaderAddress, Leader.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Chan[Transport, Acceptor[Transport]]] =
    for (acceptorAddress <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](acceptorAddress, Acceptor.serializer)

  // proposedValue holds a proposed value, if one has been proposed. Once a
  // Fast Paxos client has proposed a value, it will not propose any other
  // value.
  private var proposedValue: Option[String] = None

  // The value chosen by Fast Paxos.
  @JSExport
  protected var chosenValue: Option[String] = None

  // Phase 2b responses from the acceptors.
  @JSExport
  protected var phase2bResponses: mutable.Set[Phase2b] = mutable.Set()

  // A list of promises to fulfill once a value has been chosen.
  private var promises: mutable.Buffer[Promise[String]] = mutable.Buffer()

  // A timer to resend a proposed value. If a client doesn't hear back from
  // acceptors or from leaders quickly enough, it resends its proposal to all
  // of the leaders.
  private val reproposeTimer: Transport#Timer = timer(
    "reproposeTimer",
    java.time.Duration.ofSeconds(5),
    () => {
      proposedValue match {
        case Some(v) =>
          for (leader <- leaders) {
            leader.send(
              LeaderInbound().withProposeRequest(ProposeRequest(v = v))
            )
          }
        case None =>
          logger.fatal("Attempting to repropose, but no value was proposed.")
      }
      reproposeTimer.start()
    }
  )

  override def receive(
      src: Transport#Address,
      inbound: InboundMessage
  ): Unit = {
    import ClientInbound.Request
    inbound.request match {
      case Request.ProposeReply(r) => handleReply(src, r)
      case Request.Phase2B(r)      => handlePhase2B(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  // chooseValue(v) is called when a value v is chosen.
  private def chooseValue(chosen: String): Unit = {
    // Validate that only one value can ever be chosen.
    logger.info(s"Value '${chosen}' was chosen.")
    chosenValue match {
      case Some(oldChosen) =>
        logger.check_eq(chosen, oldChosen)
      case None =>
    }

    // Record the chosen value, fulfill all the pending promises, and stop
    // the repropose timer.
    chosenValue = Some(chosen)
    promises.foreach(_.success(chosen))
    promises.clear()
    reproposeTimer.stop()
  }

  private def handleReply(
      src: Transport#Address,
      reply: ProposeReply
  ): Unit = {
    chooseValue(reply.chosen)
  }

  private def handlePhase2B(
      src: Transport#Address,
      reply: Phase2b
  ): Unit = {
    // In this implementation of Fast Paxos, we assume that round 0 is the only
    // fast round. Thus, an acceptor responds to a client only in round 0.
    logger.check_eq(reply.round, 0)

    phase2bResponses += reply

    // Wait until we have a fast quorum of phase2b messages.
    if (phase2bResponses.size < config.fastQuorumSize) {
      return
    }

    // If we've received a fast quorum of phase 2b votes, then our proposed
    // value has been chosen.
    logger.check(proposedValue.isDefined)
    chooseValue(proposedValue.get)
  }

  private def _propose(v: String, promise: Promise[String]): Unit = {
    // If a value has already been chosen, then there's no need to propose a
    // new value. We simply call the callback immediately.
    chosenValue match {
      case Some(chosen) =>
        promise.success(chosen)
        return
      case None =>
    }

    // If a value has already been proposed, then there's no need to propose a
    // new value. We simply record the callback to be invoked once the value
    // has been chosen.
    proposedValue match {
      case Some(_) =>
        promises += promise
        return
      case None =>
    }

    // Send the value to the acceptors. If the acceptors don't respond quick
    // enough, we repropose.
    proposedValue = Some(v)
    for (acceptor <- acceptors) {
      acceptor.send(AcceptorInbound().withProposeRequest(ProposeRequest(v = v)))
    }
    reproposeTimer.start()
  }

  def propose(v: String): Future[String] = {
    val promise = Promise[String]()
    transport.executionContext.execute(() => _propose(v, promise))
    promise.future
  }
}
