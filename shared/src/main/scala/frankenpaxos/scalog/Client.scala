package frankenpaxos.scalog

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
import frankenpaxos.quorums.Grid
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
    // Resend periods.
    resendClientRequestPeriod: java.time.Duration,
    measureLatencies: Boolean
)

@JSExportAll
object ClientOptions {
  val default = ClientOptions(
    resendClientRequestPeriod = java.time.Duration.ofSeconds(10),
    measureLatencies = true
  )
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("scalog_client_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("scalog_client_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val clientRequestsSentTotal: Counter = collectors.counter
    .build()
    .name("scalog_client_client_requests_sent_total")
    .help("Total number of client requests sent.")
    .register()

  val clientRepliesReceivedTotal: Counter = collectors.counter
    .build()
    .name("scalog_client_replies_received_total")
    .help("Total number of successful replies responses received.")
    .register()

  val staleClientRepliesReceivedTotal: Counter = collectors.counter
    .build()
    .name("scalog_client_stale_client_replies_received_total")
    .help("Total number of stale client replies received.")
    .register()

  val resendClientRequestTotal: Counter = collectors.counter
    .build()
    .name("scalog_client_resend_client_request_total")
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
  type AcceptorIndex = Int

  @JSExportAll
  case class State(
      id: Id,
      command: Array[Byte],
      result: Promise[Array[Byte]],
      resendClientRequest: Transport#Timer
  )

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // The client's address. A client includes its address in its commands so
  // that replicas know where to send back the reply.
  private val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // Server channels.
  private val servers: Seq[Chan[Server[Transport]]] =
    for (shard <- config.serverAddresses; a <- shard)
      yield chan[Server[Transport]](a, Server.serializer)

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
        servers(rand.nextInt(servers.size))
          .send(ServerInbound().withClientRequest(clientRequest))
        metrics.resendClientRequestTotal.inc()
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
        servers(rand.nextInt(servers.size))
          .send(ServerInbound().withClientRequest(clientRequest))

        // Update our state.
        states(pseudonym) = State(
          id = id,
          command = command,
          result = promise,
          resendClientRequest = makeResendClientRequestTimer(clientRequest)
        )
        ids(pseudonym) = id + 1
        metrics.clientRequestsSentTotal.inc()
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request

    val label = inbound.request match {
      case Request.ClientReply(_) => "ClientReply"
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientReply(r) => handleClientReply(src, r)
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
    states.get(pseudonym) match {
      case None =>
        logger.debug(
          s"A client received a ClientReply for pseudonym ${pseudonym}, " +
            s"but the client doesn't have a state for this pseudonym. The " +
            s"ClientReply is being ignored."
        )
        metrics.staleClientRepliesReceivedTotal.inc()

      case Some(state) =>
        if (clientReply.commandId.clientId != state.id) {
          logger.debug(
            s"A client received a ClientReply for pseudonym ${pseudonym}, " +
              s"but the client id ${clientReply.commandId.clientId} doesn't " +
              s"match the expected client id ${state.id}. The " +
              s"ClientReply is being ignored."
          )
          metrics.staleClientRepliesReceivedTotal.inc()
          return
        }

        state.resendClientRequest.stop()
        state.result.success(clientReply.result.toByteArray())
        states -= pseudonym
        metrics.clientRepliesReceivedTotal.inc()
    }
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
}
