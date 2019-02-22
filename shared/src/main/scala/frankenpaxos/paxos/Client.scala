package frankenpaxos.paxos

import scala.collection.mutable.Buffer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Chan

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
  override def serializer = Client.serializer

  // The set of leaders.
  private val leaders: Seq[Chan[Transport, Leader[Transport]]] =
    for (leaderAddress <- config.leaderAddresses)
      yield
        chan[Leader[Transport]](
          leaderAddress,
          Leader.serializer
        )

  // proposedValue holds a proposed value, if one has been proposed. Once a
  // Paxos client has proposed a value, it will not propose any other value.
  private var proposedValue: Option[String] = None

  // The value chosen by Paxos.
  var chosenValue: Option[String] = None

  // A list of promises to fulfill once a value has been chosen.
  private var promises: Buffer[Promise[String]] = Buffer()

  // A timer to resend a value proposal.
  private val reproposeTimer: Transport#Timer =
    timer(
      "reproposeTimer",
      java.time.Duration.ofSeconds(5),
      () => {
        proposedValue match {
          case Some(v) => {
            for (leader <- leaders) {
              leader.send(
                LeaderInbound().withProposeRequest(ProposeRequest(v = v))
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
      inbound: ClientInbound
  ): Unit = {
    import ClientInbound.Request
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

    // Send the value to one arbitrarily chosen leader. If this leader
    // happens to be dead, we'll resend the proposal to all the leaders on
    // timeout.
    proposedValue = Some(v)
    leaders.iterator
      .next()
      .send(LeaderInbound().withProposeRequest(ProposeRequest(v = v)))
    reproposeTimer.start()
  }

  def propose(v: String): Future[String] = {
    val promise = Promise[String]()
    transport.executionContext.execute(() => _propose(v, promise))
    promise.future
  }
}
