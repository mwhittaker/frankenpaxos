package zeno.examples

import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import scala.collection.mutable.Buffer
import zeno.ProtoSerializer
import zeno.TypedActorClient

@JSExportAll
object PaxosClientInboundSerializer
    extends ProtoSerializer[PaxosClientInbound] {
  type A = PaxosClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object PaxosClientActor {
  val serializer = PaxosClientInboundSerializer
}

@JSExportAll
class PaxosClientActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    proposerAddresses: Set[Transport#Address],
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = PaxosClientInbound
  override def serializer = PaxosClientActor.serializer

  // The set of proposers.
  logger.check_ne(proposerAddresses.size, 0)
  private val proposers
    : Set[TypedActorClient[Transport, PaxosProposerActor[Transport]]] =
    for (proposerAddress <- proposerAddresses)
      yield
        typedActorClient[PaxosProposerActor[Transport]](
          proposerAddress,
          PaxosProposerActor.serializer
        )

  // valueProposed holds a proposed value, if one has been proposed. Once a
  // Paxos client has proposed a value, it will not propose any other value.
  private var proposedValue: Option[String] = None

  // The value chosen by Paxos.
  private var chosenValue: Option[String] = None

  // A list of callbacks to invoke once a value has been chosen.
  // TODO(mwhittaker): Replace with futures/promises.
  private var callbacks: Buffer[String => Unit] = Buffer()

  // A timer to resend a value proposal.
  private val reproposeTimer: Transport#Timer =
    timer(
      "reproposeTimer",
      java.time.Duration.ofSeconds(1),
      () => {
        proposedValue match {
          case Some(v) => {
            for (proposer <- proposers) {
              proposer.send(
                PaxosProposerInbound().withProposeRequest(ProposeRequest(v = v))
              )
            }
          }
          case None => {
            logger.fatal(
              "Attempting to repropose value, but no value was ever proposed."
            )
          }
        }
        reproposeTimer.start()
      }
    );

  override def receive(
      src: Transport#Address,
      inbound: PaxosClientInbound
  ): Unit = {
    import PaxosClientInbound.Request
    inbound.request match {
      case Request.ProposeReply(ProposeReply(chosen)) => {
        // Validate that only one value can ever be chosen.
        logger.info(s"Value '$chosen' was chosen.")
        chosenValue match {
          case Some(oldChosen) if oldChosen != chosen => {
            logger.fatal(
              s"Two different values were chosen: '$oldChosen' and " +
                s"then '$chosen'."
            )
          }
          case Some(_) | None => {}
        }

        // Record the chosen value, invoke all the pending callbacks, and stop
        // the repropose timer.
        chosenValue = Some(chosen)
        callbacks.foreach(_(chosen))
        callbacks.clear()
        reproposeTimer.stop()
      }
      case Request.Empty => {
        logger.fatal("Empty ProposeReply encountered.")
      }
    }
  }

  def propose(
      v: String,
      callback: String => Unit
  ): Unit = {
    // If a value has already been chosen, then there's no need to propose a
    // new value. We simply call the callback immediately.
    chosenValue match {
      case Some(chosen) => {
        callback(chosen)
        return
      }
      case None => {}
    }

    // If a value has already been proposed, then there's no need to propose a
    // new value. We simply record the callback to be invoked once the value
    // has been chosen.
    proposedValue match {
      case Some(_) => {
        callbacks += callback
        return
      }
      case None => {}
    }

    // Send the value to one arbitrarily chosen proposer. If this proposer
    // happens to be dead, we'll resend the proposal to all the proposers on
    // timeout.
    proposedValue = Some(v)
    proposers.iterator
      .next()
      .send(
        PaxosProposerInbound().withProposeRequest(ProposeRequest(v = v))
      )
    reproposeTimer.start()
  }
}
