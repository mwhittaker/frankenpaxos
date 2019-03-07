package frankenpaxos.fastmultipaxos

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.concurrent.{Future, Promise}
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
  // Fields ////////////////////////////////////////////////////////////////////
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

  // The round that this client thinks the leader is in. This value is not
  // always accurate. It's just the client's best guess. The leader associated
  // with this round can be computed using `config.roundSystem`.
  @JSExport
  protected var round: Int = 0

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
  protected var pendingCommand: Option[PendingCommand] = None

  // Leader and acceptor channels.
  private val leaders: Map[Int, Chan[Leader[Transport]]] = {
    for ((address, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](address, Leader.serializer)
  }.toMap

  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (address <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](address, Acceptor.serializer)

  // Timers ////////////////////////////////////////////////////////////////////
  // A timer to resend a proposed value. If a client doesn't hear back from
  // quickly enough, it resends its proposal to all of the leaders.
  private val reproposeTimer: Transport#Timer = timer(
    "reproposeTimer",
    // TODO(mwhittaker): Make this a parameter.
    java.time.Duration.ofSeconds(5),
    () => {
      pendingCommand match {
        case Some(PendingCommand(id, command, _)) =>
          val request = ProposeRequest(
            Command(clientAddress = addressAsBytes,
                    clientId = id,
                    command = ByteString.copyFrom(command))
          )
          for ((_, leader) <- leaders) {
            leader.send(LeaderInbound().withProposeRequest(request))
          }
        case None =>
          logger.fatal("Attempting to repropose, but no value was proposed.")
      }
      reproposeTimer.start()
    }
  )

  // Methods ///////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request
    inbound.request match {
      case Request.ProposeReply(r) => handleProposeReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def handleProposeReply(
      src: Transport#Address,
      proposeReply: ProposeReply
  ): Unit = {
    pendingCommand match {
      case Some(PendingCommand(id, command, promise)) =>
        if (proposeReply.clientId == id) {
          round = proposeReply.round
          pendingCommand = None
          reproposeTimer.stop()
          promise.success(proposeReply.result.toByteArray())
        } else {
          logger.warn(s"""Received a reply for unpending command with id
                  |'${proposeReply.clientId}'.""".stripMargin)
        }
      case None =>
        logger.warn(s"""Received a reply for unpending command with id
                |'${proposeReply.clientId}'.""".stripMargin)
    }
  }

  private def _propose(
      command: Array[Byte],
      promise: Promise[Array[Byte]]
  ): Unit = {
    pendingCommand match {
      case Some(_) =>
        val err = "You cannot propose a command while one is pending."
        promise.failure(new IllegalStateException(err))

      case None =>
        pendingCommand = Some(PendingCommand(id, command, promise))
        val request = ProposeRequest(
          Command(clientAddress = addressAsBytes,
                  clientId = id,
                  command = ByteString.copyFrom(command))
        )
        config.roundSystem.roundType(round) match {
          case ClassicRound =>
            val leader = leaders(config.roundSystem.leader(round))
            leader.send(LeaderInbound().withProposeRequest(request))
          case FastRound =>
            for (acceptor <- acceptors) {
              acceptor.send(AcceptorInbound().withProposeRequest(request))
            }
        }
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

  def propose(command: String): Future[String] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => _propose(command.getBytes(), promise)
    )
    promise.future.map(new String(_))(
      concurrent.ExecutionContext.Implicits.global
    )
  }
}
