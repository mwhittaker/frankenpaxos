package frankenpaxos.matchmakermultipaxos

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
    // If `unsafeDontUseClientTable` is true, then the replica doesn't cache
    // results in a client table. This makes the protocol unsafe, as exactly
    // once semantics is not longer ensured. This flag should only be used for
    // performance debugging.
    unsafeDontUseClientTable: Boolean,
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
    unsafeDontUseClientTable = false,
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
    .name("matchmakermultipaxos_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakermultipaxos_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val redundantlyChosenTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_replica_redundantly_chosen_total")
    .help("Total number of Chosen commands that were redundantly received.")
    .register()

  val executedLogEntriesTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_replica_executed_log_entries_total")
    .labelNames("type") // "noop" or "command"
    .help("Total number of log entries that have been executed.")
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_replica_executed_commands_total")
    .help("Total number of commands that have been executed.")
    .register()

  val reduntantlyExecutedCommandsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_replica_reduntantly_executed_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val recoversSentTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_replica_recovers_sent_total")
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
    metrics: ReplicaMetrics = new ReplicaMetrics(PrometheusCollectors)
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
  // Replica index.
  private val index = config.replicaAddresses.indexOf(address)

  // The log of commands. We implement the log as a BufferMap as opposed to
  // something like a SortedMap for efficiency. `log` is public for testing.
  @JSExport
  val log = new util.BufferMap[CommandOrNoop](options.logGrowSize)

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

  // TODO(mwhittaker): Add back.
  // A timer to send Recover messages to the leaders. The timer is optional
  // because if we set the `options.unsafeDontRecover` flag to true, then we
  // don't even bother setting up this timer.
  // private val recoverTimer: Option[Transport#Timer] =
  //   if (options.unsafeDontRecover) {
  //     None
  //   } else {
  //     Some(
  //       timer(
  //         "recover",
  //         frankenpaxos.Util.randomDuration(options.recoverLogEntryMinPeriod,
  //                                          options.recoverLogEntryMaxPeriod),
  //         () => {
  //           getProxyReplica().send(
  //             ProxyReplicaInbound().withRecover(
  //               Recover(slot = executedWatermark)
  //             )
  //           )
  //           metrics.recoversSentTotal.inc()
  //         }
  //       )
  //     )
  //   }

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

  // `executeCommand(slot, command)` attempts to execute the command `command`
  // in slot `slot`. Attempting to execute `command` may or may not produce a
  // corresponding ClientReply. If the command is stale, it may not produce a
  // ClientReply. If it isn't stale, it will produce a ClientReply.
  //
  // If a ClientReply is produced, it is sent to the client, but not always.
  // Not every replica has to send back every reply. A ClientReply only has to
  // be sent once. Thus, replica `i` will only send the ClientReply if `slot
  // % config.numReplicas == index`.
  private def executeCommand(slot: Slot, command: Command): Unit = {
    val commandId = command.commandId
    val clientIdentity = (commandId.clientAddress, commandId.clientPseudonym)
    val client = chan[Client[Transport]](
      transport.addressSerializer.fromBytes(
        commandId.clientAddress.toByteArray()
      ),
      Client.serializer
    )

    // Return early if we have already executed this command.
    clientTable.get(clientIdentity) match {
      case None =>
        // Nothing is in the client table for this client, so we haven't yet
        // executed this command.
        {}

      case Some((largestClientId, cachedResult)) =>
        if (commandId.clientId < largestClientId) {
          // We've already executed a more recent command for this client. This
          // command is stale, so we ignore it.
          metrics.reduntantlyExecutedCommandsTotal.inc()
          return
        } else if (commandId.clientId == largestClientId) {
          // We have the result of this command cached in the client table.
          // It's possible that the client never received our response. So, we
          // replay the result back to the client.
          client.send(
            ClientInbound().withClientReply(
              ClientReply(commandId = commandId, result = cachedResult)
            )
          )
          metrics.reduntantlyExecutedCommandsTotal.inc()
          return
        }
    }

    val result =
      ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
    clientTable(clientIdentity) = (commandId.clientId, result)
    if (slot % config.numReplicas == index) {
      client.send(
        ClientInbound()
          .withClientReply(ClientReply(commandId = commandId, result = result))
      )
    }
    metrics.executedCommandsTotal.inc()
  }

  private def executeCommandOrNoop(
      slot: Slot,
      commandOrNoop: CommandOrNoop
  ): Unit = {
    commandOrNoop.value match {
      case CommandOrNoop.Value.Noop(Noop()) =>
        metrics.executedLogEntriesTotal.labels("noop").inc()
      case CommandOrNoop.Value.Command(command) =>
        executeCommand(slot, command)
        metrics.executedLogEntriesTotal.labels("command").inc()
      case CommandOrNoop.Value.Empty =>
        logger.fatal("Empty CommandOrNoop encountered.")
    }
  }

  private def executeLog(): Unit = {
    while (true) {
      log.get(executedWatermark) match {
        case Some(commandOrNoop) =>
          executeCommandOrNoop(executedWatermark, commandOrNoop)
          executedWatermark += 1

        case None =>
          // We have to execute the log in prefix order, so if we hit an empty
          // slot, we have to stop executing.
          return
      }
    }
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
    // TODO(mwhittaker): Add back.
    // If `numChosen != executedWatermark`, then the recover timer is running.
    // val recoverTimerRunning = numChosen != executedWatermark

    log.get(chosen.slot) match {
      case Some(_) =>
        // We've already received a Chosen message for this slot. We ignore the
        // message.
        metrics.redundantlyChosenTotal.inc()
        return
      case None =>
        log.put(chosen.slot, chosen.value)
        numChosen += 1
    }
    executeLog()

    // TODO(mwhittaker): Add back.
    // If `numChosen != executedWatermark`, then there's a hole in the log. We
    // start or stop the timer depending on whether it is already running. If
    // `options.unsafeDontRecover`, though, we skip all this.
    // if (options.unsafeDontRecover) {
    //   // Do nothing.
    // } else if (!recoverTimerRunning && numChosen != executedWatermark) {
    //   recoverTimer.foreach(_.start())
    // } else if (recoverTimerRunning && numChosen == executedWatermark) {
    //   recoverTimer.foreach(_.stop())
    // }
  }
}
