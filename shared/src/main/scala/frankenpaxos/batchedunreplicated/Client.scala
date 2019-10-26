package frankenpaxos.batchedunreplicated

import collection.mutable
import com.google.protobuf.ByteString
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
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
case class ClientOptions()

@JSExportAll
object ClientOptions {
  val default = ClientOptions()
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("batchedunreplicated_client_requests_total")
    .help("Total number of client requests sent.")
    .register()

  val responsesTotal: Counter = collectors.counter
    .build()
    .name("batchedunreplicated_client_responses_total")
    .help("Total number of successful client responses received.")
    .register()

  val unpendingResponsesTotal: Counter = collectors.counter
    .build()
    .name("batchedunreplicated_client_unpending_responses_total")
    .help("Total number of unpending client responses received.")
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

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ClientInbound
  override val serializer = ClientInboundSerializer

  type CommandId = Int

  // Clients can only propose one request at a time (per pseudonym), so if
  // there is a pending command, no other command can be proposed. This
  // restriction hurts performance a bit---a single client cannot pipeline
  // requests---but it simplifies the design of the protocol.
  @JSExportAll
  case class PendingCommand(
      commandId: CommandId,
      command: Array[Byte],
      result: Promise[Array[Byte]]
  )

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new scala.util.Random(seed)

  // Batcher channel.
  private val batchers: Seq[Chan[Batcher[Transport]]] =
    for (address <- config.batcherAddresses)
      yield chan[Batcher[Transport]](address, Batcher.serializer)

  // The client's address. A client includes its address in its commands so
  // that the server knows where to send back the reply.
  private val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  @JSExport
  protected var nextId: Int = 0

  @JSExport
  protected var pendingCommands = mutable.Map[CommandId, PendingCommand]()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def proposeImpl(
      command: Array[Byte],
      promise: Promise[Array[Byte]]
  ): Unit = {
    val id = nextId
    nextId += 1
    pendingCommands(id) =
      PendingCommand(commandId = id, command = command, result = promise)
    val batcher = batchers(rand.nextInt(batchers.size))
    batcher.send(
      BatcherInbound().withClientRequest(
        ClientRequest(
          command = Command(clientAddress = addressAsBytes,
                            commandId = id,
                            command = ByteString.copyFrom(command))
        )
      )
    )
    metrics.requestsTotal.inc()
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request
    inbound.request match {
      case Request.ClientReply(r) => handleClientReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def handleClientReply(
      src: Transport#Address,
      clientReply: ClientReply
  ): Unit = {
    pendingCommands.get(clientReply.result.commandId) match {
      case Some(pendingCommand: PendingCommand) =>
        pendingCommand.result.success(clientReply.result.result.toByteArray())
        pendingCommands -= clientReply.result.commandId
        metrics.responsesTotal.inc()

      case None =>
        logger.debug(
          s"A client received a ClientReply for an unpending command with " +
            s"id ${clientReply.result.commandId}."
        )
        metrics.unpendingResponsesTotal.inc()
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(command: Array[Byte]): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => proposeImpl(command, promise)
    )
    promise.future
  }

  def propose(command: String): Future[String] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => proposeImpl(command.getBytes(), promise)
    )
    promise.future.map(new String(_))(
      concurrent.ExecutionContext.Implicits.global
    )
  }
}
