package frankenpaxos.epaxos

import scala.collection.mutable.Buffer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Chan

import scala.collection.mutable
import java.util.Random

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

  // The set of replicas.
  private val replicas: Seq[Chan[Transport, Replica[Transport]]] =
    for (replicaAddress <- config.replicaAddresses)
      yield
        chan[Replica[Transport]](
          replicaAddress,
          Replica.serializer
        )

  // valueProposed holds a proposed value, if one has been proposed. Once a
  // Paxos client has proposed a value, it will not propose any other value.
  // TODO(neil): This comment is out of date. -Michael.
  var proposedValue: Option[String] = None

  // The state returned to client after command was proposed
  // TODO(michael): Introduce a state machine abstraction.
  var state: String = ""

  // A list of promises to fulfill once a value has been chosen.
  private var promises: Buffer[Promise[String]] = Buffer()

  var replies: mutable.Set[String] = mutable.Set()

  private val reproposeTimer: Transport#Timer = timer(
    "reproposeTimer",
    java.time.Duration.ofSeconds(5),
    () => {
      proposedValue match {
        case Some(v) => {
          for (replica <- replicas) {
            replica.send(
              ReplicaInbound().withClientRequest(
                Request(proposedValue.get)
              )
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

  override def receive(src: Transport#Address, inbound: ClientInbound): Unit = {
    import ClientInbound.Request
    inbound.request match {
      case Request.RequestReply(r) => handleRequestReply(src, r)
      case Request.Empty => {
        logger.fatal("Empty PaxosProposerInbound encountered.")
      }
    }
  }

  private def handleRequestReply(
      address: Transport#Address,
      response: RequestReply
  ): Unit = {
    //state = response.response
    replies.add(response.commandInstance + ": " + response.command)
    reproposeTimer.stop()
  }

  private def _propose(v: String, promise: Promise[String]): Unit = {
    //println("Value is being proposed: " + v)
    proposedValue = Some(v)
    val rand = new Random(System.currentTimeMillis())
    val random_index = rand.nextInt(replicas.length)

    replicas(random_index)
      .send(
        ReplicaInbound().withClientRequest(
          Request(proposedValue.get)
        )
      )
    reproposeTimer.start()
  }

  def propose(v: String): Future[String] = {
    val promise = Promise[String]()
    transport.executionContext.execute(() => _propose(v, promise))
    promise.future
  }

}
