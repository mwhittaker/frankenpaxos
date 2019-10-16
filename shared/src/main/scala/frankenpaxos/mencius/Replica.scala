package frankenpaxos.mencius

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
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
    // Replicas inform leaders every `sendChosenWatermarkEveryNEntries` log
    // entries of the chosen watermark.
    sendChosenWatermarkEveryNEntries: Int,
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
    sendChosenWatermarkEveryNEntries = 1000,
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
    .name("mencius_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("mencius_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val executedWatermark: Gauge = collectors.gauge
    .build()
    .name("mencius_replica_executed_watermark")
    .help("The replica's executedWatermark.")
    .register()

  val redundantlyChosenTotal: Counter = collectors.counter
    .build()
    .name("mencius_replica_redundantly_chosen_total")
    .help("Total number of Chosen commands that were redundantly received.")
    .register()

  val executedLogEntriesTotal: Counter = collectors.counter
    .build()
    .name("mencius_replica_executed_log_entries_total")
    .labelNames("type") // "noop" or "command"
    .help("Total number of log entries that have been executed.")
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("mencius_replica_executed_commands_total")
    .help("Total number of commands that have been executed.")
    .register()

  val reduntantlyExecutedCommandsTotal: Counter = collectors.counter
    .build()
    .name("mencius_replica_reduntantly_executed_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val chosenWatermarksSentTotal: Counter = collectors.counter
    .build()
    .name("mencius_replica_chosen_watermarks_sent_total")
    .help("Total number of chosen watermarks sent.")
    .register()

  val recoversSentTotal: Counter = collectors.counter
    .build()
    .name("mencius_replica_recovers_sent_total")
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

  // ProxyReplica channels.
  private val proxyReplicas: Seq[Chan[ProxyReplica[Transport]]] =
    for (address <- config.proxyReplicaAddresses)
      yield chan[ProxyReplica[Transport]](address, ProxyReplica.serializer)

  // Replica index.
  logger.check(config.replicaAddresses.contains(address))
  private val index = config.replicaAddresses.indexOf(address)

  // A round system used to figure out which leader groups are in charge of
  // which slots. For example, if we have 5 leader groups and we're leader
  // group 1 and we'd like to know which slot to use after slot 20, we can call
  // slotSystem.nextClassicRound(1, 20).
  private val slotSystem =
    new RoundSystem.ClassicRoundRobin(config.numLeaderGroups)

  // The log of commands. We implement the log as a BufferMap as opposed to
  // something like a SortedMap for efficiency. `log` is public for testing.
  @JSExport
  val log =
    new util.BufferMap[CommandBatchOrNoop](options.logGrowSize)

  // Every log entry less than `executedWatermark` has been executed. There may
  // be commands larger than `executedWatermark` pending execution.
  // `executedWatermark` is public for testing.
  @JSExport
  var executedWatermark: Int = 0
  metrics.executedWatermark.set(executedWatermark)

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

  // A timer to send Recover messages to the leaders. The timer is optional
  // because if we set the `options.unsafeDontRecover` flag to true, then we
  // don't even bother setting up this timer.
  private val recoverTimer: Option[Transport#Timer] =
    if (options.unsafeDontRecover) {
      None
    } else {
      Some(
        timer(
          "recover",
          frankenpaxos.Util.randomDuration(options.recoverLogEntryMinPeriod,
                                           options.recoverLogEntryMaxPeriod),
          () => {
            getProxyReplica().send(
              ProxyReplicaInbound().withRecover(
                Recover(slot = executedWatermark)
              )
            )
            metrics.recoversSentTotal.inc()
          }
        )
      )
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

  private def getProxyReplica(): Chan[ProxyReplica[Transport]] = {
    config.distributionScheme match {
      case Hash      => proxyReplicas(rand.nextInt(proxyReplicas.size))
      case Colocated => proxyReplicas(index)
    }
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
        if (slot % config.numReplicas == index) {
          clientReplies += ClientReply(commandId = commandId, result = result)
        }
        metrics.executedCommandsTotal.inc()

      case Some((largestClientId, cachedResult)) =>
        if (commandId.clientId < largestClientId) {
          metrics.reduntantlyExecutedCommandsTotal.inc()
        } else if (commandId.clientId == largestClientId) {
          clientReplies += ClientReply(commandId = commandId,
                                       result = cachedResult)
          metrics.reduntantlyExecutedCommandsTotal.inc()
        } else {
          val result =
            ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
          clientTable(clientIdentity) = (commandId.clientId, result)
          if (slot % config.numReplicas == index) {
            clientReplies += ClientReply(commandId = commandId, result = result)
          }
          metrics.executedCommandsTotal.inc()
        }
    }
  }

  private def executeCommandBatchOrNoop(
      slot: Slot,
      commandBatchOrNoop: CommandBatchOrNoop,
      clientReplies: mutable.Buffer[ClientReply]
  ): Unit = {
    commandBatchOrNoop.value match {
      case CommandBatchOrNoop.Value.Noop(Noop()) =>
        metrics.executedLogEntriesTotal.labels("noop").inc()
      case CommandBatchOrNoop.Value.CommandBatch(batch) =>
        batch.command.foreach(executeCommand(slot, _, clientReplies))
        metrics.executedLogEntriesTotal.labels("command").inc()
      case CommandBatchOrNoop.Value.Empty =>
        logger.fatal("Empty CommandBatchOrNoop encountered.")
    }
  }

  private def executeLog(): ClientReplyBatch = {
    val clientReplies = mutable.Buffer[ClientReply]()

    while (true) {
      log.get(executedWatermark) match {
        case Some(commandBatchOrNoop) =>
          executeCommandBatchOrNoop(executedWatermark,
                                    commandBatchOrNoop,
                                    clientReplies)
          executedWatermark += 1
          metrics.executedWatermark.set(executedWatermark)

          // Replicas send a ChosenWatermark message to the leaders every
          // `options.sendChosenWatermarkEveryNEntries` command entries. The
          // responsibility of sending the ChosenWatermark message is
          // distributed round robin across the replicas.
          //
          // For example, imagine replicas want to send a ChosenWatermark
          // message every 10 commands. Then replica 1 sends it after 10
          // commands, replica 2 sends it after 20 commands, replica 0 sends it
          // after 30 commands, replica 1 sends it after 40 commands, and so
          // on.
          val mod = executedWatermark % options.sendChosenWatermarkEveryNEntries
          val div = executedWatermark / options.sendChosenWatermarkEveryNEntries
          if (mod == 0 && div % config.numReplicas == index) {
            // Select a proxy replica uniformly at random, and send the
            // ChosenWatermark message to it. The proxy replica will forward
            // the message to the leaders.
            getProxyReplica().send(
              ProxyReplicaInbound()
                .withChosenWatermark(ChosenWatermark(slot = executedWatermark))
            )
            metrics.chosenWatermarksSentTotal.inc()
          }

        case None =>
          // We have to execute the log in prefix order, so if we hit an empty
          // slot, we have to stop executing.
          return ClientReplyBatch(batch = clientReplies.toSeq)
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
        case Request.Chosen(_)          => "Chosen"
        case Request.ChosenNoopRange(_) => "ChosenNoopRange"
        case Request.Empty =>
          logger.fatal("Empty ReplicaInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Chosen(r)          => handleChosen(src, r)
        case Request.ChosenNoopRange(r) => handleChosenNoopRange(src, r)
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
    val recoverTimerRunning = numChosen != executedWatermark

    log.get(chosen.slot) match {
      case Some(_) =>
        // We've already received a Chosen message for this slot. We ignore the
        // message.
        metrics.redundantlyChosenTotal.inc()
        return
      case None =>
        log.put(chosen.slot, chosen.commandBatchOrNoop)
        numChosen += 1
    }
    val clientReplyBatch = executeLog()

    // If we have client replies, send them to a proxy replica. We choose a
    // proxy replica uniformly at random.
    //
    // TODO(mwhittaker): Add flushing?
    if (clientReplyBatch.batch.size > 0) {
      getProxyReplica().send(
        ProxyReplicaInbound().withClientReplyBatch(clientReplyBatch)
      )
    }

    // If `numChosen != executedWatermark`, then there's a hole in the log. We
    // start or stop the timer depending on whether it is already running. If
    // `options.unsafeDontRecover`, though, we skip all this.
    if (options.unsafeDontRecover) {
      // Do nothing.
    } else if (!recoverTimerRunning && numChosen != executedWatermark) {
      recoverTimer.foreach(_.start())
    } else if (recoverTimerRunning && numChosen == executedWatermark) {
      recoverTimer.foreach(_.stop())
    }
  }

  private def handleChosenNoopRange(
      src: Transport#Address,
      chosen: ChosenNoopRange
  ): Unit = {
    // If `numChosen != executedWatermark`, then the recover timer is running.
    val recoverTimerRunning = numChosen != executedWatermark

    for (slot <- chosen.slotStartInclusive
           until chosen.slotEndExclusive
           by config.numLeaderGroups) {
      log.get(slot) match {
        case Some(_) =>
          // We've already received a Chosen message for this slot. We ignore the
          // message.
          metrics.redundantlyChosenTotal.inc()
          return
        case None =>
          log.put(slot, CommandBatchOrNoop().withNoop(Noop()))
          numChosen += 1
      }
    }
    val clientReplyBatch = executeLog()

    // If we have client replies, send them to a proxy replica. We choose a
    // proxy replica uniformly at random.
    //
    // TODO(mwhittaker): Add flushing?
    if (clientReplyBatch.batch.size > 0) {
      getProxyReplica().send(
        ProxyReplicaInbound().withClientReplyBatch(clientReplyBatch)
      )
    }

    // If `numChosen != executedWatermark`, then there's a hole in the log. We
    // start or stop the timer depending on whether it is already running. If
    // `options.unsafeDontRecover`, though, we skip all this.
    if (options.unsafeDontRecover) {
      // Do nothing.
    } else if (!recoverTimerRunning && numChosen != executedWatermark) {
      recoverTimer.foreach(_.start())
    } else if (recoverTimerRunning && numChosen == executedWatermark) {
      recoverTimer.foreach(_.stop())
    }
  }
}
