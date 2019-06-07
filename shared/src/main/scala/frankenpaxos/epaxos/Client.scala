package frankenpaxos.epaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import java.util.Random
import scala.collection.mutable
import scala.collection.mutable.Buffer
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
case class ClientOptions(reproposePeriod: java.time.Duration)

@JSExportAll
object ClientOptions {
  val default = ClientOptions(
    reproposePeriod = java.time.Duration.ofSeconds(10)
  )
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ClientOptions = ClientOptions.default
) extends Actor(address, transport, logger) {
  override type InboundMessage = ClientInbound
  override val serializer = ClientInboundSerializer

  val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // Every request that a client sends is annotated with a monotonically
  // increasing client id. Here, we assume that if a client fails, it does not
  // recover, so we are safe to intialize the id to 0. If clients can recover
  // from failure, we would have to implement some mechanism to ensure that
  // client ids increase over time, even after crashes and restarts.
  @JSExport
  protected var id: Int = 0

  // A pending command. Clients can only propose one request at a time, so if
  // there is a pending command, no other command can be proposed. This
  // restriction hurts performance a bit---a single client cannot pipeline
  // requests---but it simplifies the design of the protocol.
  @JSExportAll
  case class PendingCommand(
      id: Int,
      command: Array[Byte],
      result: Promise[Array[Byte]]
  )

  @JSExport
  var pendingCommand: Option[PendingCommand] = None

  // Replica channels.
  private val replicas: Map[Int, Chan[Replica[Transport]]] = {
    for ((address, i) <- config.replicaAddresses.zipWithIndex)
      yield i -> chan[Replica[Transport]](address, Replica.serializer)
  }.toMap

  // Timers ////////////////////////////////////////////////////////////////////
  // A timer to resend a proposed value. If a client doesn't hear back from a
  // replica quickly enough, it resends its proposal to all of the replicas.
  private val reproposeTimer: Transport#Timer = timer(
    "reproposeTimer",
    options.reproposePeriod,
    () => {
      pendingCommand match {
        case None =>
          logger.fatal("Attempting to repropose, but no value was proposed.")

        case Some(pendingCommand) =>
          val request = toClientRequest(pendingCommand)
          for ((_, replica) <- replicas) {
            replica.send(ReplicaInbound().withClientRequest(request))
          }
      }
      reproposeTimer.start()
    }
  )

  // Methods ///////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request
    inbound.request match {
      case Request.ClientReply(r) => handleClientReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def toClientRequest(pendingCommand: PendingCommand): ClientRequest = {
    val PendingCommand(id, command, _) = pendingCommand
    ClientRequest(
      Command(clientAddress = addressAsBytes,
              clientId = id,
              command = ByteString.copyFrom(command))
    )
  }

  private def sendProposeRequest(pendingCommand: PendingCommand): Unit = {
    // TODO(mwhittaker): Abstract out the policy of which replica to send to.
    val rand = new Random(System.currentTimeMillis())
    val random_index = rand.nextInt(replicas.size)
    val replica = replicas(random_index)

    val request = toClientRequest(pendingCommand)
    replica.send(ReplicaInbound().withClientRequest(request))
  }

  private def handleClientReply(
      src: Transport#Address,
      clientReply: ClientReply
  ): Unit = {
    pendingCommand match {
      case Some(PendingCommand(id, command, promise)) =>
        if (clientReply.clientId == id) {
          pendingCommand = None
          reproposeTimer.stop()
          promise.success(clientReply.result.toByteArray)
        } else {
          logger.warn(
            s"Received a reply for unpending command with id " +
              s"'${clientReply.clientId}'."
          )
        }

      case None =>
        logger.warn(
          s"Received a reply for unpending command with id " +
            s"'${clientReply.clientId}'."
        )
    }
  }

  private def _propose(
      command: Array[Byte],
      promise: Promise[Array[Byte]]
  ): Unit = {
    pendingCommand match {
      case Some(_) =>
        promise.failure(
          new IllegalStateException(
            "You cannot propose a command while one is pending."
          )
        )

      case None =>
        pendingCommand = Some(PendingCommand(id, command, promise))
        sendProposeRequest(pendingCommand.get)
        reproposeTimer.start()
        id += 1
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(command: Array[Byte]): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(() => _propose(command, promise))
    promise.future
  }

  def propose(command: String): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => _propose(command.getBytes(), promise)
    )
    promise.future
  }
}
