package frankenpaxos.horizontal

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
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
    .name("horizontal_client_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("horizontal_client_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val clientRequestsSentTotal: Counter = collectors.counter
    .build()
    .name("horizontal_client_client_requests_sent_total")
    .help("Total number of client requests sent.")
    .register()

  val clientRepliesReceivedTotal: Counter = collectors.counter
    .build()
    .name("horizontal_client_replies_received_total")
    .help("Total number of successful replies responses received.")
    .register()

  val staleClientRepliesReceivedTotal: Counter = collectors.counter
    .build()
    .name("horizontal_client_stale_client_replies_received_total")
    .help("Total number of stale client replies received.")
    .register()

  val resendClientRequestTotal: Counter = collectors.counter
    .build()
    .name("horizontal_client_resend_client_request_total")
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

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (address <- config.leaderAddresses)
      yield chan[Leader[Transport]](address, Leader.serializer)

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // The round that this client thinks the leader is in. This value is not
  // always accurate. It's just the client's best guess. The leader associated
  // with this round can be computed using `roundSystem`. The clients need to
  // know who the leader is because they need to know where to send their
  // commands.
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

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendClientRequestTimer(
      clientRequest: ClientRequest
  ): Transport#Timer = {
    val commandId = clientRequest.command.commandId
    lazy val t: Transport#Timer = timer(
      s"resendClientRequest [pseudonym=${commandId.clientPseudonym}; " +
        s"id=${commandId.clientId}]",
      options.resendClientRequestPeriod,
      () => {
        // The leader may have failed and a new leader may have taken over. We
        // broadcast leader info requests to learn of the new leader.
        leaders.foreach(
          _.send(LeaderInbound().withLeaderInfoRequest(LeaderInfoRequest()))
        )

        // We also resend our command to all of the leaders.
        leaders.foreach(
          _.send(LeaderInbound().withClientRequest(clientRequest))
        )
        metrics.resendClientRequestTotal.inc()

        t.start()
      }
    )
    t.start()
    t
  }

  // Helpers ///////////////////////////////////////////////////////////////////
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
        val leader = leaders(roundSystem.leader(round))
        leader.send(LeaderInbound().withClientRequest(clientRequest))
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
      case Request.ClientReply(r)     => handleClientReply(src, r)
      case Request.NotLeader(r)       => handleNotLeader(src, r)
      case Request.LeaderInfoReply(r) => handleLeaderInfoReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
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
          metrics.clientRepliesReceivedTotal.inc()
        } else {
          logger.debug(
            s"A client received a ClientReply for an unpending command with " +
              s"pseudonym ${clientReply.commandId.clientPseudonym} and id " +
              s"${clientReply.commandId.clientId}."
          )
          metrics.staleClientRepliesReceivedTotal.inc()
        }

      case None =>
        logger.debug(
          s"A client received a ClientReply for an unpending command with " +
            s"pseudonym ${clientReply.commandId.clientPseudonym} and id " +
            s"${clientReply.commandId.clientId}."
        )
        metrics.staleClientRepliesReceivedTotal.inc()
    }
  }

  private def handleNotLeader(
      src: Transport#Address,
      notLeader: NotLeader
  ): Unit = {
    leaders.foreach(
      _.send(LeaderInbound().withLeaderInfoRequest(LeaderInfoRequest()))
    )
  }

  private def handleLeaderInfoReply(
      src: Transport#Address,
      leaderInfo: LeaderInfoReply
  ): Unit = {
    if (leaderInfo.round <= round) {
      logger.debug(
        s"A client received a LeaderInfoReply message with round " +
          s"${leaderInfo.round} but is already in round $round. The " +
          s"LeaderInfoReply message must be stale, so we are ignoring it."
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
    val leader = leaders(roundSystem.leader(newRound))
    if (roundSystem.leader(oldRound) != roundSystem.leader(newRound)) {
      for ((pseudonym, pendingCommand) <- pendingCommands) {
        leader.send(
          LeaderInbound().withClientRequest(toClientRequest(pendingCommand))
        )
        resendClientRequestTimers(pseudonym).reset()
      }
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
