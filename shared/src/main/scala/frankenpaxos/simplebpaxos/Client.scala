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
    .help("Total number of times a client reproposes a value..")
    .register()
}

@JSExportAll
object Client {
  val serializer = ClientInboundSerializer

  type Pseudonym = Int
  type Id = Int

  // A pending command. Clients can only propose one request at a time, so if
  // there is a pending command, no other command can be proposed. This
  // restriction hurts performance a bit---a single client cannot pipeline
  // requests---but it simplifies the design of the protocol.
  @JSExportAll
  case class PendingCommand(
      pseudonym: Pseudonym,
      id: Int,
      command: Array[Byte],
      result: Promise[Array[Byte]]
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
  val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // Random number generator.
  val rand = new Random(seed)

  // Every request that a client sends is annotated with a monotonically
  // increasing client id. Here, we assume that if a client fails, it does not
  // recover, so we are safe to intialize the id to 0. If clients can recover
  // from failure, we would have to implement some mechanism to ensure that
  // client ids increase over time, even after crashes and restarts.
  @JSExport
  protected var ids = mutable.Map[Pseudonym, Id]()

  @JSExport
  protected var pendingCommands = mutable.Map[Pseudonym, PendingCommand]()

  // Leader channels.
  private val leaders: Map[Int, Chan[Leader[Transport]]] = {
    for ((address, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](address, Leader.serializer)
  }.toMap

  // Timers to resend a proposed value. If a client doesn't hear back from a
  // leader quickly enough, it resends its proposal.
  private val reproposeTimers = mutable.Map[Pseudonym, Transport#Timer]()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def toClientRequest(pendingCommand: PendingCommand): ClientRequest = {
    val PendingCommand(pseudonym, id, command, _) = pendingCommand
    ClientRequest(
      command = Command(clientPseudonym = pseudonym,
                        clientAddress = addressAsBytes,
                        clientId = id,
                        command = ByteString.copyFrom(command))
    )
  }

  private def sendProposeRequest(pendingCommand: PendingCommand): Unit = {
    // TODO(mwhittaker): Abstract out the policy of which leader to send to.
    val randomIndex = rand.nextInt(leaders.size)
    val leader = leaders(randomIndex)
    val request = toClientRequest(pendingCommand)
    leader.send(LeaderInbound().withClientRequest(request))
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
              s"can only have one pending request at a time. Try waiting or s" +
              s"use a different pseudonym."
          )
        )

      case None =>
        // Send the command.
        val id = ids.getOrElse(pseudonym, 0)
        val pendingCommand = PendingCommand(pseudonym, id, command, promise)
        sendProposeRequest(pendingCommand)

        // Update our metadata.
        pendingCommands(pseudonym) = pendingCommand
        reproposeTimers
          .getOrElseUpdate(pseudonym, makeReproposeTimer(pseudonym))
          .start()
        ids(pseudonym) = id + 1
    }
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeReproposeTimer(pseudonym: Pseudonym): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"reproposeTimer (pseudonym $pseudonym)",
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
            // EPaxos doesn't have a mechanism to prevent dueling leaders, we
            // send to one leader at a time.
            sendProposeRequest(pendingCommand)
        }
        t.start()
      }
    )
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
      case Request.Empty => {
        logger.fatal("Empty ClientInbound encountered.")
      }
    }
  }

  private def handleClientReply(
      src: Transport#Address,
      clientReply: ClientReply
  ): Unit = {
    pendingCommands.get(clientReply.clientPseudonym) match {
      case Some(PendingCommand(pseudonym, id, command, promise)) =>
        logger.checkEq(clientReply.clientPseudonym, pseudonym)
        if (clientReply.clientId == id) {
          pendingCommands -= pseudonym
          reproposeTimers(pseudonym).stop()
          promise.success(clientReply.result.toByteArray)
          metrics.responsesTotal.inc()
        } else {
          logger.warn(
            s"Received a reply for unpending command with pseudonym " +
              s"${clientReply.clientPseudonym} and id ${clientReply.clientId}."
          )
          metrics.unpendingResponsesTotal.inc()
        }

      case None =>
        logger.warn(
          s"Received a reply for unpending command with pseudonym " +
            s"${clientReply.clientPseudonym} and id ${clientReply.clientId}."
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
