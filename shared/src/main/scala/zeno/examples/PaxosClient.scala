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
    transport: Transport,
    logger: Logger,
    config: PaxosConfig[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = PaxosClientInbound
  override def serializer = PaxosClientActor.serializer

  // The set of proposers.
  private val proposers
    : Seq[TypedActorClient[Transport, PaxosProposerActor[Transport]]] =
    for (proposerAddress <- config.proposerAddresses)
      yield
        typedActorClient[PaxosProposerActor[Transport]](
          proposerAddress,
          PaxosProposerActor.serializer
        )

  // valueProposed holds a proposed value, if one has been proposed. Once a
  // Paxos client has proposed a value, it will not propose any other value.
  private var proposedValue: Option[String] = None

  // The value chosen by Paxos.
  var chosenValue: Option[String] = None

  // A list of promises to fulfill once a value has been chosen.
  // TODO(mwhittaker): Replace with futures/promises.
  private var promises: Buffer[Promise[String]] = Buffer()

  // A timer to resend a value proposal.
  private val reproposeTimer: Transport#Timer =
    timer(
      "reproposeTimer",
      java.time.Duration.ofSeconds(5),
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
            logger.warn(
              s"Two different values were chosen: '$oldChosen' and " +
                s"then '$chosen'."
            )
          }
          case Some(_) | None => {}
        }

        // Record the chosen value, fulfill all the pending promises, and stop
        // the repropose timer.
        chosenValue = Some(chosen)
        promises.foreach(_.success(chosen))
        promises.clear()
        reproposeTimer.stop()
      }
      case Request.Empty => {
        logger.fatal("Empty ProposeReply encountered.")
      }
    }
  }

  private def _propose(v: String, promise: Promise[String]): Unit = {
    // If a value has already been chosen, then there's no need to propose a
    // new value. We simply call the callback immediately.
    chosenValue match {
      case Some(chosen) => {
        promise.success(chosen)
        return
      }
      case None => {}
    }

    // If a value has already been proposed, then there's no need to propose a
    // new value. We simply record the callback to be invoked once the value
    // has been chosen.
    proposedValue match {
      case Some(_) => {
        promises += promise
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

  def propose(v: String): Future[String] = {
    val promise = Promise[String]()
    transport.executionContext.execute(() => _propose(v, promise))
    promise.future
  }
}
