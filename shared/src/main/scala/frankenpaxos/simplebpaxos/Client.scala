package frankenpaxos.simplebpaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import scala.collection.mutable
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
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_client_requests_total")
    .help(
      "Total number of client requests sent to consensus (including those " +
        "sent in batches)."
    )
    .register()

  val requestBatchesTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_client_request_batches_total")
    .help("Total number of client request batches sent to consensus.")
    .register()

  val responsesTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_client_responses_total")
    .help("Total number of successful client responses.")
    .register()

  val unpendingResponsesTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_client_unpending_responses_total")
    .help("Total number of unpending client responses.")
    .register()

  val reproposeTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_client_repropose_total")
    .help("Total number of times a client reproposes a value.")
    .register()
}

@JSExportAll
object Client {
  val serializer = ClientInboundSerializer

  type Pseudonym = Int
  type Id = Int

  // DO_NOT_SUBMIT(mwhittaker): Document.
  @JSExportAll
  case class Timing(
      startTime: java.time.Instant,
      stopTime: java.time.Instant,
      durationNanos: Long
  )

  // DO_NOT_SUBMIT(mwhittaker): Document.
  @JSExportAll
  case class PendingCommand(
      pseudonym: Pseudonym,
      id: Int,
      command: Array[Byte],
      startTime: java.time.Instant,
      startTimeNanos: Long,
      batch: PendingCommandBatch
  )

  // DO_NOT_SUBMIT(mwhittaker): Document.
  @JSExportAll
  case class PendingCommandBatch(
      pseudonyms: Set[Pseudonym],
      pendingPseudonyms: mutable.Set[Pseudonym],
      results: mutable.Map[Pseudonym, (Array[Byte], Timing)],
      promise: Promise[mutable.Map[Pseudonym, (Array[Byte], Timing)]]
  )
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
  import Client._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ClientInbound
  override def serializer = Client.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Random number generator.
  val rand = new Random(seed)

  // The client's address, as a ByteString.
  val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for ((address, i) <- config.leaderAddresses.zipWithIndex)
      yield chan[Leader[Transport]](address, Leader.serializer)

  // The id of every pseudonym. Every request that a client sends is annotated
  // with a monotonically increasing client id. Here, we assume that if a
  // client fails, it does not recover, so we are safe to intialize the id to
  // 0. If clients can recover from failure, we would have to implement some
  // mechanism to ensure that client ids increase over time, even after crashes
  // and restarts.
  @JSExport
  protected var ids = mutable.Map[Pseudonym, Id]()

  // DO_NOT_SUBMIT(mwhittaker): Document.
  @JSExport
  protected var pendingCommands = mutable.Map[Pseudonym, PendingCommand]()

  // DO_NOT_SUBMIT(mwhittaker): Document.
  @JSExport
  protected var pendingPseudonyms = mutable.Set[Pseudonym]()

  // Timers to resend a proposed value. If a client doesn't hear back from a
  // leader quickly enough, it resends its proposal.
  private val reproposeTimers = mutable.Map[Pseudonym, Transport#Timer]()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def getLeader(): Chan[Leader[Transport]] = {
    // TODO(mwhittaker): Abstract out the policy of which leader to send to.
    leaders(rand.nextInt(leaders.size))
  }

  private def toClientRequest(pendingCommand: PendingCommand): ClientRequest = {
    ClientRequest(
      command = Command(
        clientPseudonym = pendingCommand.pseudonym,
        clientAddress = addressAsBytes,
        clientId = pendingCommand.id,
        command = ByteString.copyFrom(pendingCommand.command)
      )
    )
  }

  private def proposeBatchImpl(
      commands: Map[Pseudonym, Array[Byte]],
      promise: Promise[mutable.Map[Pseudonym, (Array[Byte], Timing)]]
  ): Unit = {
    // Check that none of the proposed pseudonyms are currently pending
    // the completion of a batch.
    if (!pendingPseudonyms.intersect(commands.keySet).isEmpty) {
      promise.failure(
        new IllegalStateException(
          s"You attempted to propose a batch with pseudonyms " +
            s"${commands.keys}, but there is a batch of pending commands " +
            s"with one of those pseudonyms. A pseudonym can only have one " +
            s"pending batch at a time. Try waiting or use a different " +
            s"pseudonym."
        )
      )
      return
    }

    // Form the batch.
    val batch = PendingCommandBatch(
      pseudonyms = commands.keySet,
      pendingPseudonyms = mutable.Set() ++ commands.keys,
      results = mutable.Map[Pseudonym, (Array[Byte], Timing)](),
      promise = promise
    )

    // Update metadata.
    val startTime = java.time.Instant.now()
    val startTimeNanos = System.nanoTime()
    for ((pseudonym, command) <- commands) {
      logger.check(!pendingCommands.contains(pseudonym))
      val id = ids.getOrElse(pseudonym, 0)
      ids(pseudonym) = id + 1

      pendingCommands(pseudonym) = PendingCommand(
        pseudonym = pseudonym,
        id = id,
        command = command,
        startTime = startTime,
        startTimeNanos = startTimeNanos,
        batch = batch
      )
      pendingPseudonyms += pseudonym
      reproposeTimers(pseudonym) = makeReproposeTimer(pseudonym)
    }

    // Send the command batch.
    getLeader().send(
      LeaderInbound().withClientRequestBatch(
        ClientRequestBatch(
          clientRequest =
            commands.keys.map(k => toClientRequest(pendingCommands(k))).toSeq
        )
      )
    )
    metrics.requestsTotal.inc(commands.size)
    metrics.requestBatchesTotal.inc()
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeReproposeTimer(pseudonym: Pseudonym): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"reproposeTimer [$pseudonym]",
      options.reproposePeriod,
      () => {
        metrics.reproposeTotal.inc()
        pendingCommands.get(pseudonym) match {
          case None =>
            logger.fatal(
              s"Attempting to repropose pending command for pseudonym " +
                s"$pseudonym, but there is no pending command."
            )

          case Some(pendingCommand) =>
            // Ideally, we would re-send our request to all leaders. But, since
            // Simple BPaxos doesn't have a mechanism to prevent dueling
            // leaders, we send to one leader at a time.
            getLeader().send(
              LeaderInbound().withClientRequest(toClientRequest(pendingCommand))
            )
        }
        t.start()
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ClientInbound
  ): Unit = {
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
    pendingCommands.get(clientReply.clientPseudonym) match {
      case None =>
        logger.debug(
          s"Client received a reply for unpending command with pseudonym " +
            s"${clientReply.clientPseudonym} and id ${clientReply.clientId}."
        )
        metrics.unpendingResponsesTotal.inc()

      case Some(pendingCommand: PendingCommand) =>
        logger.checkEq(clientReply.clientPseudonym, pendingCommand.pseudonym)
        if (clientReply.clientId != pendingCommand.id) {
          logger.debug(
            s"Client received a reply for unpending command with pseudonym " +
              s"${clientReply.clientPseudonym} and id ${clientReply.clientId}."
          )
          metrics.unpendingResponsesTotal.inc()
          return
        }

        // Clean up the pending command.
        metrics.responsesTotal.inc()
        pendingCommands -= pendingCommand.pseudonym
        reproposeTimers(pendingCommand.pseudonym).stop()
        reproposeTimers -= pendingCommand.pseudonym

        // Update the batch.
        val batch = pendingCommand.batch
        val stopTime = java.time.Instant.now()
        val stopTimeNanos = System.nanoTime()
        batch.results(pendingCommand.pseudonym) = (
          clientReply.result.toByteArray,
          Timing(
            startTime = pendingCommand.startTime,
            stopTime = stopTime,
            durationNanos = stopTimeNanos - pendingCommand.startTimeNanos
          )
        )

        batch.pendingPseudonyms -= pendingCommand.pseudonym
        if (batch.pendingPseudonyms.isEmpty) {
          // If the batch is done, clean up and return to the client.
          pendingPseudonyms --= batch.pseudonyms
          batch.promise.success(batch.results)
        }
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def proposeBatch(
      commands: Map[Pseudonym, Array[Byte]]
  ): Future[mutable.Map[Pseudonym, (Array[Byte], Timing)]] = {
    val promise = Promise[mutable.Map[Pseudonym, (Array[Byte], Timing)]]()
    transport.executionContext.execute(
      () => proposeBatchImpl(commands, promise)
    )
    promise.future
  }

  def propose(
      pseudonym: Pseudonym,
      command: Array[Byte]
  ): Future[(Array[Byte], Timing)] = {
    implicit val context = transport.executionContext
    proposeBatch(Map(pseudonym -> command)).map(m => m(pseudonym))
  }

  def propose(
      pseudonym: Pseudonym,
      command: String
  ): Future[(String, Timing)] = {
    implicit val context = transport.executionContext
    propose(pseudonym, command.getBytes()).map({
      case (bytes, timing) => (new String(bytes), timing)
    })
  }
}
