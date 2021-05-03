package frankenpaxos.scalog

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.util
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object ReplicaInboundSerializer extends ProtoSerializer[ReplicaInbound] {
  type A = ReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer
}

@JSExportAll
case class ReplicaOptions(
    // A Replica implements its log as a BufferMap. `logGrowSize` is the
    // `growSize` argument used to construct the BufferMap.
    logGrowSize: Int,
    // When a replica receives and executes a batch of chosen commands, it has
    // two options for how to reply to the clients. If `batchFlush` is false,
    // then the replica sends back the replies one at a time without any sort
    // of batching or flushing. If `batchFlush` is true, then the replica
    // writes all of the replies and then flushes all its channels.
    batchFlush: Boolean,
    // If a replica has a hole in its log for a certain amount of time, it
    // sends a Recover message to the leader (indirectly through a replica
    // proxy). The time to recover is chosen uniformly at random from the range
    // defined by these two options.
    recoverLogEntryMinPeriod: java.time.Duration,
    recoverLogEntryMaxPeriod: java.time.Duration,
    // If `unsafeDontRecover` is true, replicas don't make any attempt to
    // recover log entries. This is not live and should only be used for
    // performance debugging.
    unsafeDontRecover: Boolean,
    measureLatencies: Boolean
)

@JSExportAll
object ReplicaOptions {
  val default = ReplicaOptions(
    logGrowSize = 5000,
    batchFlush = false,
    recoverLogEntryMinPeriod = java.time.Duration.ofMillis(5000),
    recoverLogEntryMaxPeriod = java.time.Duration.ofMillis(10000),
    unsafeDontRecover = false,
    measureLatencies = true
  )
}

@JSExportAll
class ReplicaMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("scalog_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("scalog_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("scalog_replica_executed_commands_total")
    .help("Total number of commands that have been executed.")
    .register()

  val redundantlyChosenTotal: Counter = collectors.counter
    .build()
    .name("scalog_replica_redundantly_chosen_total")
    .help("Total number of Chosen commands that were redundantly received.")
    .register()

  val reduntantlyExecutedCommandsTotal: Counter = collectors.counter
    .build()
    .name("scalog_replica_reduntantly_executed_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val recoversSentTotal: Counter = collectors.counter
    .build()
    .name("scalog_replica_recovers_sent_total")
    .help("Total number of recover messages sent.")
    .register()
}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    // Public for Javascript visualizations.
    val stateMachine: StateMachine,
    config: Config[Transport],
    options: ReplicaOptions = ReplicaOptions.default,
    metrics: ReplicaMetrics = new ReplicaMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.replicaAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReplicaInbound
  override val serializer = ReplicaInboundSerializer

  type ClientId = Int
  type ClientPseudonym = Int
  type Slot = Int

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // Aggregator channel.
  private val aggregator: Chan[Aggregator[Transport]] =
    chan[Aggregator[Transport]](config.aggregatorAddress, Aggregator.serializer)

  // Client channels.
  @JSExport
  protected val clients =
    mutable.Map[Transport#Address, Chan[Client[Transport]]]()

  // Proxy Replica channels.
  private val proxyReplicas: Seq[Chan[ProxyReplica[Transport]]] =
    for (a <- config.proxyReplicaAddresses)
      yield chan[ProxyReplica[Transport]](a, ProxyReplica.serializer)

  // Replica index.
  private val index = config.replicaAddresses.indexOf(address)

  // The log of commands. We implement the log as a BufferMap as opposed to
  // something like a SortedMap for efficiency. `log` is public for testing.
  @JSExport
  val log = new util.BufferMap[Command](options.logGrowSize)

  // Every log entry less than `executedWatermark` has been executed. There may
  // be commands larger than `executedWatermark` pending execution.
  // `executedWatermark` is public for testing.
  @JSExport
  var executedWatermark: Int = 0

  // The number of log entries that have been chosen and placed in `log`. We
  // use `numChosen` and `executedWatermark` to know whether there are commands
  // pending execution. If `numChosen == executedWatermark`, then all chosen
  // commands have been executed. Otherwise, there are commands waiting to get
  // executed.
  @JSExport
  protected var numChosen: Int = 0

  // The client table used to ensure exactly once execution semantics. Every
  // entry in the client table is keyed by a clients address and its pseudonym
  // and maps to the largest executed id for the client and the result of
  // executing the command. Note that unlike with generalized protocols like
  // BPaxos and EPaxos, we don't need to use the more complex ClientTable
  // class. A simple map suffices.
  @JSExport
  protected var clientTable =
    mutable.Map[(ByteString, ClientPseudonym), (ClientId, ByteString)]()

  // A timer to send Recover messages to the aggregator. The timer is optional
  // because if we set the `options.unsafeDontRecover` flag to true, then we
  // don't even bother setting up this timer.
  private val recoverTimer: Option[Transport#Timer] =
    if (options.unsafeDontRecover) {
      None
    } else {
      lazy val t: Transport#Timer =
        timer(
          "recover",
          frankenpaxos.Util.randomDuration(options.recoverLogEntryMinPeriod,
                                           options.recoverLogEntryMaxPeriod),
          () => {
            aggregator.send(
              AggregatorInbound().withRecover(Recover(slot = executedWatermark))
            )
            metrics.recoversSentTotal.inc()
            t.start()
          }
        )
      Some(t)
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

  private def clientChan(commandId: CommandId): Chan[Client[Transport]] = {
    val clientAddress = transport.addressSerializer.fromBytes(
      commandId.clientAddress.toByteArray()
    )
    clients.getOrElseUpdate(
      clientAddress,
      chan[Client[Transport]](clientAddress, Client.serializer)
    )
  }

  // `executeCommand(slot, command, clientReplies)` attempts to execute the
  // command `command` in slot `slot`. Attempting to execute `command` may or
  // may not produce a corresponding ClientReply. If the command is stale, it
  // may not produce a ClientReply. If it isn't stale, it will produce a
  // ClientReply.
  //
  // If a ClientReply is produced, it is appended to the end of
  // `clientReplies`, but not always. Not every replica has to send back every
  // reply. A ClientReply only has to be sent once. Thus, replica `i` will
  // only append the ClientReply if `slot % config.numReplicas == index`.
  private def executeCommand(
      slot: Slot,
      command: Command,
      clientReplies: mutable.Buffer[ClientReply]
  ): Unit = {
    val commandId = command.commandId
    val clientIdentity = (commandId.clientAddress, commandId.clientPseudonym)
    clientTable.get(clientIdentity) match {
      case None =>
        val result =
          ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
        clientTable(clientIdentity) = (commandId.clientId, result)
        if (slot % config.replicaAddresses.size == index) {
          clientReplies += ClientReply(commandId = commandId,
                                       slot = slot,
                                       result = result)
        }
        metrics.executedCommandsTotal.inc()

      case Some((largestClientId, cachedResult)) =>
        if (commandId.clientId < largestClientId) {
          metrics.reduntantlyExecutedCommandsTotal.inc()
        } else if (commandId.clientId == largestClientId) {
          clientReplies += ClientReply(commandId = commandId,
                                       slot = slot,
                                       result = cachedResult)
          metrics.reduntantlyExecutedCommandsTotal.inc()
        } else {
          val result =
            ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
          clientTable(clientIdentity) = (commandId.clientId, result)
          if (slot % config.replicaAddresses.size == index) {
            clientReplies += ClientReply(commandId = commandId,
                                         slot = slot,
                                         result = result)
          }
          metrics.executedCommandsTotal.inc()
        }
    }
  }

  private def executeLog(): mutable.Buffer[ClientReply] = {
    val clientReplies = mutable.Buffer[ClientReply]()

    while (true) {
      log.get(executedWatermark) match {
        case None =>
          // We have to execute the log in prefix order, so if we hit an empty
          // slot, we have to stop executing.
          return clientReplies

        case Some(command) =>
          val slot = executedWatermark
          executeCommand(slot, command, clientReplies)
          executedWatermark += 1
      }
    }

    // The above for loop will always return a ClientReplyBatch, but Scala
    // cannot figure that out automatically, so we need to raise an exception
    // here to placate the type checker.
    throw new IllegalStateException()
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ReplicaInbound.Request

    val label =
      inbound.request match {
        case Request.Chosen(_) => "Chosen"
        case Request.Empty =>
          logger.fatal("Empty ReplicaInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Chosen(r) => handleChosen(src, r)
        case Request.Empty =>
          logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  private def handleChosen(
      src: Transport#Address,
      chosen: Chosen
  ): Unit = {
    // If `numChosen != executedWatermark`, then the recover timer is running.
    val isRecoverTimerRunning = numChosen != executedWatermark
    val oldExecutedWatermark = executedWatermark

    // Insert the chosen commands into our log.
    timed("handleChosen (insert commands)") {
      for ((command, i) <- chosen.commandBatch.command.zipWithIndex) {
        val slot = i + chosen.slot

        log.get(slot) match {
          case Some(_) =>
            // We've already received a Chosen message for this slot. We ignore
            // the message.
            metrics.redundantlyChosenTotal.inc()
          case None =>
            log.put(slot, command)
            numChosen += 1
        }
      }
    }

    // Execute the log.
    val replies = timed("handleChosen (execute log)") {
      executeLog()
    }

    // Send replies back to the clients or to a proxy replica.
    timed("handleChosen (send replies)") {
      (config.proxyReplicaAddresses.size == 0, options.batchFlush) match {
        case (true, true) =>
          for (reply <- replies) {
            val client = clientChan(reply.commandId)
            client.sendNoFlush(ClientInbound().withClientReply(reply))
          }
          clients.values.foreach(_.flush())

        case (true, false) =>
          for (reply <- replies) {
            val client = clientChan(reply.commandId)
            client.send(ClientInbound().withClientReply(reply))
          }

        case (false, _) =>
          val proxyReplica = proxyReplicas(rand.nextInt(proxyReplicas.size))
          proxyReplica.send(
            ProxyReplicaInbound()
              .withClientReplyBatch(ClientReplyBatch(replies))
          )
      }
    }

    // The recover timer should be running if there are more chosen commands
    // than executed commands. If the recover timer should be running, it's
    // either for a slot for which the timer was already running
    // (shouldRecoverTimerBeReset = false) or for a new slot for which the
    // timer wasn't yet running (shouldRecoverTimerBeReset = true).
    val shouldRecoverTimerBeRunning = numChosen != executedWatermark
    val shouldRecoverTimerBeReset = oldExecutedWatermark != executedWatermark
    if (options.unsafeDontRecover) {
      // Do nothing.
    } else if (isRecoverTimerRunning) {
      (shouldRecoverTimerBeRunning, shouldRecoverTimerBeReset) match {
        case (true, true)  => recoverTimer.foreach(_.reset())
        case (true, false) => // Do nothing.
        case (false, _)    => recoverTimer.foreach(_.stop())
      }
    } else if (shouldRecoverTimerBeRunning) {
      recoverTimer.foreach(_.start())
    }
  }
}
