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

import com.google.protobuf.ByteString

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
      result: Promise[Instance]
  )

  @JSExport
  var pendingCommand: Option[PendingCommand] = None

  // Leader and acceptor channels.
  private val replicas: Map[Int, Chan[Replica[Transport]]] = {
    for ((address, i) <- config.replicaAddresses.zipWithIndex)
      yield i -> chan[Replica[Transport]](address, Replica.serializer)
  }.toMap

  val executionMap: mutable.Map[Command, String] = mutable.Map()

  // Timers ////////////////////////////////////////////////////////////////////
  // A timer to resend a proposed value. If a client doesn't hear back from
  // quickly enough, it resends its proposal to all of the leaders.
  private val reproposeTimer: Transport#Timer = timer(
    "reproposeTimer",
    // TODO(mwhittaker): Make this a parameter.
    java.time.Duration.ofSeconds(10),
    () => {
      pendingCommand match {
        case None =>
          logger.fatal("Attempting to repropose, but no value was proposed.")

        case Some(pendingCommand) =>
          val request = toProposeRequest(pendingCommand)
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
      case Request.RequestReply(r) => handleProposeReply(src, r)
      case Request.ReadReply(r) => handleReadReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def toProposeRequest(
      pendingCommand: PendingCommand
  ): Request = {
    val PendingCommand(id, command, _) = pendingCommand
    Request(
      Command(clientAddress = addressAsBytes,
              clientId = id,
              command = ByteString.copyFrom(command))
    )
  }

  private def sendProposeRequest(pendingCommand: PendingCommand): Unit = {
    val request = toProposeRequest(pendingCommand)
    val rand = new Random(System.currentTimeMillis())
    val random_index = rand.nextInt(replicas.size)

    val replica = replicas(random_index)
    replica.send(ReplicaInbound().withClientRequest(request))
  }

  private def sendReadRequest(command: Command, instance: Instance): Unit = {
    val replica = replicas(instance.leaderIndex)
    logger.info("Sending replica message: " + instance.leaderIndex)
    replica.send(ReplicaInbound().withRead(Read(
      instance,
      command
    )))
  }

  private def handleProposeReply(
      src: Transport#Address,
      proposeReply: RequestReply
  ): Unit = {
    pendingCommand match {
      case Some(PendingCommand(id, command, promise)) =>
        if (proposeReply.command.clientId == id) {
          pendingCommand = None
          reproposeTimer.stop()
          promise.success(proposeReply.commandInstance)
          sendReadRequest(proposeReply.command, proposeReply.commandInstance)
        } else {
          logger.warn(s"""Received a reply for unpending command with id
            |'${proposeReply.command.clientId}'.""".stripMargin)
        }
      case None =>
        logger.warn(s"""Received a reply for unpending command with id
          |'${proposeReply.command.clientId}'.""".stripMargin)
    }
  }

  private def handleReadReply(address: Transport#Address, readReply: ReadReply): Unit = {
    executionMap.put(readReply.command, readReply.state)
    //println(readReply.state)
    logger.info("State is " + readReply.state)
  }

  private def _propose(
      command: Array[Byte],
      promise: Promise[Instance]
  ): Unit = {
    pendingCommand match {
      case Some(_) =>
        val err = "You cannot propose a command while one is pending."
        promise.failure(new IllegalStateException(err))

      case None =>
        pendingCommand = Some(PendingCommand(id, command, promise))
        sendProposeRequest(pendingCommand.get)
        reproposeTimer.start()
        id += 1
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(command: Array[Byte]): Future[Instance] = {
    val promise = Promise[Instance]()
    transport.executionContext.execute(() => _propose(command, promise))
    promise.future
  }

  def propose(command: String): Future[Instance] = {
    val promise = Promise[Instance]()
    transport.executionContext.execute(
      () => _propose(command.getBytes(), promise)
    )
    promise.future
  }
}
