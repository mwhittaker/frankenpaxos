package frankenpaxos.fastmultipaxos

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
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
case class ClientOptions(
    reproposePeriod: java.time.Duration
)

@JSExportAll
object ClientOptions {
  val default = ClientOptions(
    reproposePeriod = java.time.Duration.ofSeconds(10)
  )
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {
  val responsesTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_client_responses_total")
    .help("Total number of successful client responses.")
    .register()

  val unpendingResponsesTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_client_unpending_responses_total")
    .help("Total number of unpending client responses.")
    .register()

  val reproposeTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_client_repropose_total")
    .help("Total number of times a client reproposes a value..")
    .register()
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ClientOptions = ClientOptions.default,
    metrics: ClientMetrics = new ClientMetrics(PrometheusCollectors)
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
    options.reproposePeriod,
    () => {
      pendingCommand match {
        case None =>
          logger.fatal("Attempting to repropose, but no value was proposed.")

        case Some(pendingCommand) =>
          logger.debug(s"Reproposing pending value in round $round.")
          val request = toProposeRequest(pendingCommand)
          for ((_, leader) <- leaders) {
            leader.send(LeaderInbound().withProposeRequest(request))
          }
      }
      reproposeTimer.start()
    }
  )

  // Methods ///////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request
    inbound.request match {
      case Request.LeaderInfo(r)   => handleLeaderInfo(src, r)
      case Request.ProposeReply(r) => handleProposeReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def toProposeRequest(
      pendingCommand: PendingCommand
  ): ProposeRequest = {
    val PendingCommand(id, command, _) = pendingCommand
    ProposeRequest(
      round = round,
      Command(clientAddress = addressAsBytes,
              clientId = id,
              command = ByteString.copyFrom(command))
    )
  }

  private def sendProposeRequest(pendingCommand: PendingCommand): Unit = {
    val request = toProposeRequest(pendingCommand)
    config.roundSystem.roundType(round) match {
      case ClassicRound =>
        val leader = leaders(config.roundSystem.leader(round))
        leader.send(LeaderInbound().withProposeRequest(request))
      case FastRound =>
        for (acceptor <- acceptors) {
          acceptor.send(AcceptorInbound().withProposeRequest(request))
        }
    }
  }

  private def handleLeaderInfo(
      src: Transport#Address,
      leaderInfo: LeaderInfo
  ): Unit = {
    (pendingCommand, leaderInfo.round > round) match {
      case (_, false) =>
      // Ignore older rounds.

      case (None, true) =>
        // We update our round, but no command is pending, so we don't have to
        // do anything.
        logger.info(
          s"Client transitioning from round $round to ${leaderInfo.round}."
        )
        round = leaderInfo.round

      case (Some(pendingCommand), true) =>
        // Update our round and re-send the pending command with the new round
        // information.
        logger.info(
          s"Client transitioning from round $round to ${leaderInfo.round}. " +
            s"Resending pending command to leader."
        )
        round = leaderInfo.round
        sendProposeRequest(pendingCommand)
        reproposeTimer.reset()
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
          metrics.responsesTotal.inc()
        } else {
          logger.warn(
            s"Received a reply for unpending command with id " +
              s"'${proposeReply.clientId}'."
          )
          metrics.unpendingResponsesTotal.inc()
        }
      case None =>
        logger.warn(
          s"Received a reply for unpending command with id " +
            s"'${proposeReply.clientId}'."
        )
        metrics.unpendingResponsesTotal.inc()
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
