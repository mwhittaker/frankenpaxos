package frankenpaxos.spaxosdecouple

import com.google.protobuf.ByteString
import frankenpaxos.{Actor, Logger, ProtoSerializer}
import frankenpaxos.monitoring.{Collectors, Counter, PrometheusCollectors}

import scala.collection.mutable
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

  type Pseudonym = Int
  type Id = Int

  val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // The round that this client thinks the leader is in. This value is not
  // always accurate. It's just the client's best guess. The leader associated
  // with this round can be computed using `config.roundSystem`.
  @JSExport
  protected var round: Int = 0

  // Every request that a client sends is annotated with a monotonically
  // increasing client id. Here, we assume that if a client fails, it does not
  // recover, so we are safe to intialize the id to 0. If clients can recover
  // from failure, we would have to implement some mechanism to ensure that
  // client ids increase over time, even after crashes and restarts.
  @JSExport
  protected var ids = mutable.Map[Pseudonym, Id]()

  // A pending command. Clients can only propose one request at a time, so if
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

// Leader and acceptor channels.
  private val proposers: Map[Int, Chan[Proposer[Transport]]] = {
    for ((address, i) <- config.proposerAddresses.zipWithIndex)
      yield i -> chan[Proposer[Transport]](address, Proposer.serializer)
  }.toMap

  // Timers ////////////////////////////////////////////////////////////////////
  // A timer to resend a proposed value. If a client doesn't hear back quickly
  // enough, it resends its proposal to all of the leaders.
  private val reproposeTimers = mutable.Map[Pseudonym, Transport#Timer]()

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    inbound.request match {
      case Request.ClientReply(r) => handleProposeReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def handleProposeReply(
      src: Transport#Address,
      proposeReply: ClientReply
  ): Unit = {
    pendingCommands.get(proposeReply.clientPseudonym) match {
      case Some(PendingCommand(pseudonym, id, command, promise)) =>
        logger.checkEq(proposeReply.clientPseudonym, pseudonym)
        if (proposeReply.clientId == id) {
          pendingCommands -= pseudonym
          reproposeTimers(pseudonym).stop()
          promise.success(proposeReply.result.toByteArray())
          metrics.responsesTotal.inc()
        } else {
          logger.warn(
            s"Received a reply for unpending command with pseudonym " +
              s"${proposeReply.clientPseudonym} and id ${proposeReply.clientId}."
          )
          metrics.unpendingResponsesTotal.inc()
        }

      case None =>
        logger.warn(
          s"Received a reply for unpending command with pseudonym " +
            s"${proposeReply.clientPseudonym} and id ${proposeReply.clientId}."
        )
        metrics.unpendingResponsesTotal.inc()
    }

  }

  // Methods ///////////////////////////////////////////////////////////////////
  private def processNewRound(newRound: Int): Unit = {
    if (newRound <= round) {
      logger.debug(
        s"Client heard about round ${newRound} but is already in round $round."
      )
      return
    }

    // If we were in an old round, we update our round information and resend
    // all our requests.
    round = newRound
    for ((pseudonym, pendingCommand) <- pendingCommands) {
      sendProposeRequest(pendingCommand)
      reproposeTimers(pseudonym).reset()
    }
  }

  private def toProposeRequest(
      pendingCommand: PendingCommand
  ): ClientRequest = {
    val PendingCommand(pseudonym, id, command, _) = pendingCommand
    ClientRequest(
      UniqueId(clientAddress = addressAsBytes,
              clientPseudonym = pseudonym,
              clientId = id),
              command = ByteString.copyFrom(command)
    )
  }

  private def sendProposeRequest(pendingCommand: PendingCommand): Unit = {
    val request = toProposeRequest(pendingCommand)
    val r = scala.util.Random
    val index = r.nextInt(config.proposerAddresses.size)
    val leader = proposers(index)
    leader.send(ProposerInbound().withClientRequest(request))
  }

  private def reproposeTimer(pseudonym: Pseudonym): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"reproposeTimer$pseudonym",
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
            val request = toProposeRequest(pendingCommand)
            for ((_, leader) <- proposers) {
              leader.send(ProposerInbound().withClientRequest(request))
            }
        }
        t.start()
      }
    )
    t
  }

  private def _propose(
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
          .getOrElseUpdate(pseudonym, reproposeTimer(pseudonym))
          .start()
        ids(pseudonym) = id + 1
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(
      pseudonym: Pseudonym,
      command: Array[Byte]
  ): Future[Array[Byte]] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => _propose(pseudonym, command, promise)
    )
    promise.future
  }

  def propose(pseudonym: Pseudonym, command: String): Future[String] = {
    val promise = Promise[Array[Byte]]()
    transport.executionContext.execute(
      () => _propose(pseudonym, command.getBytes(), promise)
    )
    promise.future.map(new String(_))(
      concurrent.ExecutionContext.Implicits.global
    )
  }
}
