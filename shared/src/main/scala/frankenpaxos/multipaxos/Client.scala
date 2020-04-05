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
    resendClientRequestPeriod: java.time.Duration,
    resendMaxSlotRequestsPeriod: java.time.Duration,
    resendReadRequestPeriod: java.time.Duration,
    // If `unsafeReadAtFirstSlot` is true, all ReadRequests are issued in slot
    // 0. With this option enabled, our protocol is not safe. Reads are no
    // longer linearizable. This should be used only for performance debugging.
    unsafeReadAtFirstSlot: Boolean,
    // To perform a linearizable quorum read, a client contacts a quorum of
    // acceptors and asks them for the largest log entry in which they have
    // voted. It then computes the maximum of these log entries; let's call
    // this value i. It issues the read at i + n where n is the number of
    // acceptor groups. If `unsafeReadAtI` is true, the client instead issues
    // the read at index i. If unsafeReadAtFirstSlot is true, we instead read
    // at 0.
    unsafeReadAtI: Boolean,
    measureLatencies: Boolean
)

@JSExportAll
object ClientOptions {
  val default = ClientOptions(
    resendClientRequestPeriod = java.time.Duration.ofSeconds(10),
    resendMaxSlotRequestsPeriod = java.time.Duration.ofSeconds(10),
    resendReadRequestPeriod = java.time.Duration.ofSeconds(10),
    unsafeReadAtFirstSlot = false,
    unsafeReadAtI = false,
    measureLatencies = true
  )
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_client_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val clientRequestsSentTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_client_requests_sent_total")
    .help("Total number of client requests sent.")
    .register()

  val clientRepliesReceivedTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_replies_received_total")
    .help("Total number of successful replies responses received.")
    .register()

  val staleClientRepliesReceivedTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_stale_client_replies_received_total")
    .help("Total number of stale client replies received.")
    .register()

  val resendClientRequestTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_resend_client_request_total")
    .help("Total number of times a client resends a ClientRequest.")
    .register()

  val resendMaxSlotRequestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_resend_max_slot_requests_total")
    .help("Total number of times a client resends a MaxSlotRequest.")
    .register()

  val resendReadRequestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_client_resend_read_requests_total")
    .help("Total number of times a client resends a ReadRequest.")
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
  type AcceptorIndex = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case class PendingWrite(
      pseudonym: Pseudonym,
      id: Id,
      command: Array[Byte],
      result: Promise[Array[Byte]],
      resendClientRequest: Transport#Timer
  ) extends State

  @JSExportAll
  case class MaxSlot(
      pseudonym: Pseudonym,
      id: Id,
      command: Array[Byte],
      result: Promise[Array[Byte]],
      maxSlotReplies: mutable.Map[AcceptorIndex, MaxSlotReply],
      resendMaxSlotRequests: Transport#Timer
  ) extends State

  @JSExportAll
  case class PendingRead(
      pseudonym: Pseudonym,
      id: Id,
      command: Array[Byte],
      result: Promise[Array[Byte]],
      resendReadRequest: Transport#Timer
  ) extends State

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
    for (a <- config.batcherAddresses)
      yield chan[Batcher[Transport]](a, Batcher.serializer)

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Seq[Chan[Acceptor[Transport]]]] =
    for (group <- config.acceptorAddresses) yield {
      for (a <- group)
        yield chan[Acceptor[Transport]](a, Acceptor.serializer)
    }

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

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
  @JSExport
  protected var states = mutable.Map[Pseudonym, State]()

  // Helpers ///////////////////////////////////////////////////////////////////
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

  private def makeResendMaxSlotRequestsTimer(
      pseudonym: Pseudonym,
      id: Id,
      group: Seq[Chan[Acceptor[Transport]]],
      maxSlotRequest: MaxSlotRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendMaxSlotRequest [pseudonym=${pseudonym}; id=${id}]",
      options.resendMaxSlotRequestsPeriod,
      () => {
        group.foreach(
          _.send(AcceptorInbound().withMaxSlotRequest(maxSlotRequest))
        )
        metrics.resendMaxSlotRequestsTotal.inc()
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendReadRequestTimer(
      pseudonym: Pseudonym,
      id: Id,
      readRequest: ReadRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendReadRequest [pseudonym=${pseudonym}; id=${id}]",
      options.resendReadRequestPeriod,
      () => {
        val replica = replicas(rand.nextInt(replicas.size))
        replica.send(ReplicaInbound().withReadRequest(readRequest))
        metrics.resendReadRequestsTotal.inc()
        t.start()
      }
    )
    t.start()
    t
  }

  // Helpers ///////////////////////////////////////////////////////////////////
  private def timed[T](label: String)(e: => T): T = {
    if (options.measureLatencies) {
      val startNanos = System.nanoTime
      val x = e
      val stopNanos = System.nanoTime
      metrics.requestsLatency
        .labels(label)
        .observe((stopNanos - startNanos).toDouble / 1000000)
      x
    } else {
      e
    }
  }

  private def getBatcher(): Chan[Batcher[Transport]] = {
    config.distributionScheme match {
      case Hash      => batchers(rand.nextInt(batchers.size))
      case Colocated => batchers(roundSystem.leader(round))
    }
  }

  private def sendClientRequest(clientRequest: ClientRequest): Unit = {
    if (config.numBatchers == 0) {
      // If there are no batchers, then we send to who we think the leader is.
      val leader = leaders(roundSystem.leader(round))
      leader.send(LeaderInbound().withClientRequest(clientRequest))
    } else {
      // If there are batchers, then we send to a randomly selected batcher.
      // The batchers will take care of forwarding our message to a leader.
      //
      // TODO(mwhittaker): Abstract out the policy that determines which
      // batcher we propose to.
      getBatcher().send(BatcherInbound().withClientRequest(clientRequest))
    }
  }

  private def writeImpl(
      pseudonym: Pseudonym,
      command: Array[Byte],
      promise: Promise[Array[Byte]]
  ): Unit = {
    states.get(pseudonym) match {
      case Some(_) =>
        promise.failure(
          new IllegalStateException(
            s"You attempted to issue a write with pseudonym $pseudonym, " +
              s"but this pseudonym already has a request pending. A client " +
              s"can only have one pending request at a time. Try waiting or " +
              s"use a different pseudonym."
          )
        )

      case None =>
        // Send the command.
        val id = ids.getOrElse(pseudonym, 0)
        val clientRequest = ClientRequest(
          command = Command(
            commandId = CommandId(clientAddress = addressAsBytes,
                                  clientPseudonym = pseudonym,
                                  clientId = id),
            command = ByteString.copyFrom(command)
          )
        )
        sendClientRequest(clientRequest)

        // Update our state.
        states(pseudonym) = PendingWrite(
          pseudonym = pseudonym,
          id = id,
          command = command,
          result = promise,
          resendClientRequest = makeResendClientRequestTimer(clientRequest)
        )
        ids(pseudonym) = id + 1
        metrics.clientRequestsSentTotal.inc()
    }
  }

  private def readImpl(
      pseudonym: Pseudonym,
      command: Array[Byte],
      promise: Promise[Array[Byte]]
  ): Unit = {
    states.get(pseudonym) match {
      case Some(_) =>
        promise.failure(
          new IllegalStateException(
            s"You attempted to issue a read with pseudonym $pseudonym, " +
              s"but this pseudonym already has a request pending. A client " +
              s"can only have one pending request at a time. Try waiting or " +
              s"use a different pseudonym."
          )
        )

      case None =>
        // Send the MaxSlotRequests to a random quorum of acceptors (thrifty by
        // default).
        val id = ids.getOrElse(pseudonym, 0)
        val maxSlotRequest = MaxSlotRequest(
          commandId = CommandId(clientAddress = addressAsBytes,
                                clientPseudonym = pseudonym,
                                clientId = id)
        )
        val group = acceptors(rand.nextInt(acceptors.size))
        val quorum = scala.util.Random.shuffle(group).take(config.f + 1)
        quorum.foreach(
          _.send(AcceptorInbound().withMaxSlotRequest(maxSlotRequest))
        )

        // Update our state.
        states(pseudonym) = MaxSlot(
          pseudonym = pseudonym,
          id = id,
          command = command,
          result = promise,
          maxSlotReplies = mutable.Map(),
          resendMaxSlotRequests =
            makeResendMaxSlotRequestsTimer(pseudonym, id, group, maxSlotRequest)
        )
        ids(pseudonym) = id + 1
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request

    val label = inbound.request match {
      case Request.ClientReply(_)           => "ClientReply"
      case Request.MaxSlotReply(_)          => "MaxSlotReply"
      case Request.ReadReply(_)             => "ReadReply"
      case Request.NotLeaderClient(_)       => "NotLeaderClient"
      case Request.LeaderInfoReplyClient(_) => "LeaderInfoReplyClient"
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientReply(r) =>
          handleClientReply(src, r)
        case Request.MaxSlotReply(r) =>
          handleMaxSlotReply(src, r)
        case Request.ReadReply(r) =>
          handleReadReply(src, r)
        case Request.NotLeaderClient(r) =>
          handleNotLeaderClient(src, r)
        case Request.LeaderInfoReplyClient(r) =>
          handleLeaderInfoReplyClient(src, r)
        case Request.Empty =>
          logger.fatal("Empty ClientInbound encountered.")
      }
    }
  }

  private def handleClientReply(
      src: Transport#Address,
      clientReply: ClientReply
  ): Unit = {
    val pseudonym = clientReply.commandId.clientPseudonym
    val state = states.get(pseudonym)
    state match {
      case None | Some(_: MaxSlot) | Some(_: PendingRead) =>
        logger.debug(
          s"A client received a ClientReply, but the state is $state. The " +
            s"ClientReply is being ignored."
        )
        metrics.staleClientRepliesReceivedTotal.inc()

      case Some(pendingWrite: PendingWrite) =>
        logger.checkEq(pseudonym, pendingWrite.pseudonym)

        if (clientReply.commandId.clientId != pendingWrite.id) {
          logger.debug(
            s"A client received a ClientReply for pseudonym ${pseudonym}, " +
              s"but the client id ${clientReply.commandId.clientId} doesn't " +
              s"match the expected client id ${pendingWrite.id}. The " +
              s"ClientReply is being ignored."
          )
          metrics.staleClientRepliesReceivedTotal.inc()
          return
        }

        pendingWrite.resendClientRequest.stop()
        pendingWrite.result.success(clientReply.result.toByteArray())
        states -= pendingWrite.pseudonym
        metrics.clientRepliesReceivedTotal.inc()
    }
  }

  private def handleMaxSlotReply(
      src: Transport#Address,
      maxSlotReply: MaxSlotReply
  ): Unit = {
    val pseudonym = maxSlotReply.commandId.clientPseudonym
    val state = states.get(pseudonym)
    state match {
      case None | Some(_: PendingWrite) | Some(_: PendingRead) =>
        logger.debug(
          s"A client received a MaxSlotReply, but the state is $state. The " +
            s"MaxSlotReply is being ignored."
        )

      case Some(maxSlot: MaxSlot) =>
        logger.checkEq(pseudonym, maxSlot.pseudonym)

        if (maxSlotReply.commandId.clientId != maxSlot.id) {
          logger.debug(
            s"A client received a MaxSlotReply for pseudonym ${pseudonym}, " +
              s"but the client id ${maxSlotReply.commandId.clientId} doesn't " +
              s"match the expected client id ${maxSlot.id}. The " +
              s"MaxSlotReply is being ignored."
          )
          return
        }

        // Wait until we have f+1 responses.
        maxSlot.maxSlotReplies(maxSlotReply.acceptorIndex) = maxSlotReply
        if (maxSlot.maxSlotReplies.size < config.f + 1) {
          return
        }

        // Compute the slot.
        //
        // TODO(mwhittaker): Double check that the `- 1` is safe.
        val slot = if (options.unsafeReadAtFirstSlot) {
          0
        } else if (options.unsafeReadAtI) {
          maxSlot.maxSlotReplies.values.map(_.slot).max
        } else {
          maxSlot.maxSlotReplies.values.map(_.slot).max + acceptors.size - 1
        }

        // Send the read.
        val readRequest = ReadRequest(
          slot = slot,
          command = Command(
            commandId = CommandId(clientAddress = addressAsBytes,
                                  clientPseudonym = pseudonym,
                                  clientId = maxSlot.id),
            command = ByteString.copyFrom(maxSlot.command)
          )
        )
        val replica = replicas(rand.nextInt(replicas.size))
        replica.send(ReplicaInbound().withReadRequest(readRequest))

        // Update our state.
        states(pseudonym) = PendingRead(
          pseudonym = pseudonym,
          id = maxSlot.id,
          command = maxSlot.command,
          result = maxSlot.result,
          resendReadRequest = makeResendReadRequestTimer(
            pseudonym,
            maxSlot.id,
            readRequest
          )
        )
        maxSlot.resendMaxSlotRequests.stop()
    }
  }

  private def handleReadReply(
      src: Transport#Address,
      readReply: ReadReply
  ): Unit = {
    val pseudonym = readReply.commandId.clientPseudonym
    val state = states.get(pseudonym)
    state match {
      case None | Some(_: PendingWrite) | Some(_: MaxSlot) =>
        logger.debug(
          s"A client received a ReadReply, but the state is $state. The " +
            s"ReadReply is being ignored."
        )

      case Some(pendingRead: PendingRead) =>
        logger.checkEq(pseudonym, pendingRead.pseudonym)

        if (readReply.commandId.clientId != pendingRead.id) {
          logger.debug(
            s"A client received a ReadReply for pseudonym ${pseudonym}, " +
              s"but the client id ${readReply.commandId.clientId} doesn't " +
              s"match the expected client id ${pendingRead.id}. The " +
              s"ReadReply is being ignored."
          )
          return
        }

        pendingRead.resendReadRequest.stop()
        pendingRead.result.success(readReply.result.toByteArray())
        states -= pseudonym
    }
  }

  private def handleNotLeaderClient(
      src: Transport#Address,
      notLeader: NotLeaderClient
  ): Unit = {
    leaders.foreach(
      _.send(
        LeaderInbound().withLeaderInfoRequestClient(LeaderInfoRequestClient())
      )
    )
  }

  private def handleLeaderInfoReplyClient(
      src: Transport#Address,
      leaderInfo: LeaderInfoReplyClient
  ): Unit = {
    if (leaderInfo.round <= round) {
      logger.debug(
        s"A client received a LeaderInfoReplyClient message with round " +
          s"${leaderInfo.round} but is already in round $round. The " +
          s"LeaderInfoReplyClient message must be stale, so we are ignoring it."
      )
      return
    }

    // Update our round.
    val oldRound = round
    val newRound = leaderInfo.round
    round = leaderInfo.round

    // TODO(mwhittaker): We may want to re-send our writes here.
  }

  // Interface /////////////////////////////////////////////////////////////////
  def write(pseudonym: Pseudonym, command: Array[Byte]): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => writeImpl(pseudonym, command, promise)
    )
    promise.future
  }

  def write(pseudonym: Pseudonym, command: String): Future[String] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => writeImpl(pseudonym, command.getBytes(), promise)
    )
    promise.future.map(new String(_))(
      concurrent.ExecutionContext.Implicits.global
    )
  }

  def read(pseudonym: Pseudonym, command: Array[Byte]): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => readImpl(pseudonym, command, promise)
    )
    promise.future
  }

  def read(pseudonym: Pseudonym, command: String): Future[String] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => readImpl(pseudonym, command.getBytes(), promise)
    )
    promise.future.map(new String(_))(
      concurrent.ExecutionContext.Implicits.global
    )
  }
}
