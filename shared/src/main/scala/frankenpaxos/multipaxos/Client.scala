package frankenpaxos.multipaxos

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.roundsystem.ClassicRound
import frankenpaxos.roundsystem.FastRound
import frankenpaxos.roundsystem.RoundSystem
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import scala.util.Random

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
    resendClientRequestPeriod: java.time.Duration
)

@JSExportAll
object ClientOptions {
  val default = ClientOptions(
    resendClientRequestPeriod = java.time.Duration.ofSeconds(10)
  )
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_requests_total")
    .help("Total number of client requests sent.")
    .register()

  val responsesTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_responses_total")
    .help("Total number of successful client responses received.")
    .register()

  val unpendingResponsesTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_unpending_responses_total")
    .help("Total number of unpending client responses received.")
    .register()

  val resendClientRequestTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_resend_client_request_total")
    .help("Total number of times a client resends a ClientRequest.")
    .register()
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ClientOptions = ClientOptions.default,
    metrics: ClientMetrics = new ClientMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ClientInbound
  override val serializer = ClientInboundSerializer

  type Pseudonym = Int
  type Id = Int

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // The client's address. A client includes its address in its commands so
  // that replicas know where to send back the reply.
  private val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // Batcher channels.
  private val batchers: Seq[Chan[Batcher[Transport]]] =
    for ((address, i) <- config.batcherAddresses.zipWithIndex)
      yield i -> chan[Batcher[Transport]](address, Batcher.serializer)

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for ((address, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](address, Leader.serializer)

  // The round that this client thinks the leader is in. This value is not
  // always accurate. It's just the client's best guess. The leader associated
  // with this round can be computed using `config.roundSystem`. The clients
  // need to know who the leader is because they need to know where to send
  // their commands.
  @JSExport
  protected var round: Int = 0

  // Every request that a client sends is annotated with a monotonically
  // increasing client id. Here, we assume that if a client fails, it does not
  // recover, so we are safe to intialize the id to 0. If clients can recover
  // from failure, we would have to implement some mechanism to ensure that
  // client ids increase over time, even after crashes and restarts.
  @JSExport
  protected var ids = mutable.Map[Pseudonym, Id]()

  // Clients can only propose one request at a time (per pseudonym), so if
  // there is a pending command, no other command can be proposed. This
  // restriction hurts performance a bit---a single client cannot pipeline
  // requests---but it simplifies the design of the protocol.
  @JSExportAll
  case class PendingCommand(
      pseudonym: Pseudonym,
      id: Id,
      command: Array[Byte],
      result: Promise[Array[Byte]]
  )

  @JSExport
  protected var pendingCommands = mutable.Map[Pseudonym, PendingCommand]()

  // A timer to resend a proposed value. If a client doesn't hear back quickly
  // enough, it resends its proposal to all of the batchers (or leaders).
  private val resendClientRequestTimers =
    mutable.Map[Pseudonym, Transport#Timer]()

  // Methods ///////////////////////////////////////////////////////////////////
  def toClientRequest(pendingCommand: PendingCommand): ClientRequest = {
    ClientRequest(
      command = Command(
        commandId = CommandId(clientAddress = addressAsBytes,
                              clientPseudonym = pendingCommand.pseudonym,
                              clientId = pendingCommand.id),
        command = ByteString.copyFrom(pendingCommand.command)
      )
    )
  }

  private def sendClientRequest(clientRequest: ClientRequest): Unit = {
    if (config.numBatchers == 0) {
      // If there are no batchers, then we send to who we think the leader is.
      val leader = leaders(config.roundSystem.leader(round))
      leader.send(LeaderInbound().withClientRequest(clientRequest))
    } else {
      // If there are batchers, then we send to a randomly selected batcher.
      // The batchers will take care of forwarding our message to a leader.
      //
      // TODO(mwhittaker): Abstract out the policy that determines which
      // batcher we propose to.
      val batcher = batchers(rand.nextInt(batchers.size))
      batcher.send(BatcherInbound().withClientRequest(clientRequest))
    }
  }

  private def makeResendClientRequestTimer(
      clientRequest: ClientRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendClientRequest " +
        s"[pseudonym=${clientRequest.command.commandId.clientPseudonym}; " +
        s"id=${clientRequest.command.commandId.clientId}]",
      options.resendClientRequestPeriod,
      () => {
        sendClientRequest(clientRequest)
        metrics.resendClientRequestTotal.inc()
        t.start()
      }
    )
    t.start()
    t
  }

  private def proposeImpl(
      pseudonym: Pseudonym,
      command: Array[Byte],
      promise: Promise[Array[Byte]]
  ): Unit = {
    pendingCommands.get(pseudonym) match {
      case Some(_) =>
        promise.failure(
          new IllegalStateException(
            s"You attempted to propose a value with pseudonym $pseudonym, " +
              s"but this pseudonym already has a command pending. A client " +
              s"can only have one pending request at a time. Try waiting or " +
              s"use a different pseudonym."
          )
        )

      case None =>
        // Send the command.
        val id = ids.getOrElse(pseudonym, 0)
        val pendingCommand = PendingCommand(pseudonym = pseudonym,
                                            id = id,
                                            command = command,
                                            result = promise)
        val clientRequest = toClientRequest(pendingCommand)
        sendClientRequest(clientRequest)
        metrics.requestsTotal.inc()

        // Update our metadata.
        pendingCommands(pseudonym) = pendingCommand
        resendClientRequestTimers(pseudonym) = makeResendClientRequestTimer(
          clientRequest
        )
        ids(pseudonym) = id + 1
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request
    inbound.request match {
      case Request.LeaderInfo(r)  => handleLeaderInfo(src, r)
      case Request.ClientReply(r) => handleClientReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def handleLeaderInfo(
      src: Transport#Address,
      leaderInfo: LeaderInfo
  ): Unit = {
    if (leaderInfo.round <= round) {
      logger.debug(
        s"A client received a LeaderInfo message with round " +
          s"${leaderInfo.round} but is already in round $round. The " +
          s"LeaderInfo message must be stale, so we are ignoring it."
      )
      return
    }

    // Update our round.
    val oldRound = round
    val newRound = leaderInfo.round
    round = leaderInfo.round

    // We've sent all of our pending commands to the leader of round `round`,
    // but we just learned about a new round `leaderInfo.round`. If the leader
    // of the new round is different than the leader of the old round, then we
    // have to re-send our messages.
    if (config.roundSystem.leader(oldRound) !=
          config.roundSystem.leader(newRound)) {
      for ((pseudonym, pendingCommand) <- pendingCommands) {
        sendClientRequest(toClientRequest(pendingCommand))
        resendClientRequestTimers(pseudonym).reset()
      }
    }
  }

  private def handleClientReply(
      src: Transport#Address,
      clientReply: ClientReply
  ): Unit = {
    pendingCommands.get(clientReply.commandId.clientPseudonym) match {
      case Some(pendingCommand: PendingCommand) =>
        logger.checkEq(clientReply.commandId.clientPseudonym,
                       pendingCommand.pseudonym)
        if (clientReply.commandId.clientId == pendingCommand.id) {
          pendingCommands -= pendingCommand.pseudonym
          resendClientRequestTimers(pendingCommand.pseudonym).stop()
          pendingCommand.result.success(clientReply.result.toByteArray())
          metrics.responsesTotal.inc()
        } else {
          logger.debug(
            s"A client received a ClientReply for an unpending command with " +
              s"pseudonym ${clientReply.commandId.clientPseudonym} and id " +
              s"${clientReply.commandId.clientId}."
          )
          metrics.unpendingResponsesTotal.inc()
        }

      case None =>
        logger.debug(
          s"A client received a ClientReply for an unpending command with " +
            s"pseudonym ${clientReply.commandId.clientPseudonym} and id " +
            s"${clientReply.commandId.clientId}."
        )
        metrics.unpendingResponsesTotal.inc()
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(
      pseudonym: Pseudonym,
      command: Array[Byte]
  ): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => proposeImpl(pseudonym, command, promise)
    )
    promise.future
  }

  def propose(pseudonym: Pseudonym, command: String): Future[String] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => proposeImpl(pseudonym, command.getBytes(), promise)
    )
    promise.future.map(new String(_))(
      concurrent.ExecutionContext.Implicits.global
    )
  }
}
