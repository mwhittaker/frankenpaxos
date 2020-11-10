package frankenpaxos.multipaxos

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
    .name("multipaxos_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val redundantlyChosenTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_redundantly_chosen_total")
    .help("Total number of Chosen commands that were redundantly received.")
    .register()

  val executedLogEntriesTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_executed_log_entries_total")
    .labelNames("type") // "noop" or "command"
    .help("Total number of log entries that have been executed.")
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_executed_commands_total")
    .help("Total number of commands that have been executed.")
    .register()

  val reduntantlyExecutedCommandsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_reduntantly_executed_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val chosenWatermarksSentTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_chosen_watermarks_sent_total")
    .help("Total number of chosen watermarks sent.")
    .register()

  val recoversSentTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_recovers_sent_total")
    .help("Total number of recover messages sent.")
    .register()

  val deferredReadsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_deferred_reads_total")
    .help("Total number of reads that have been deferred.")
    .register()

  val deferredReadLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_replica_deferred_read_latency")
    .help("Latency (in milliseconds) that a deferred read is deferred.")
    .register()

  val deferredReadBatchSize: Summary = collectors.summary
    .build()
    .name("multipaxos_replica_deferred_read_batch_size")
    .help("Size of a deferred read batch.")
    .register()

  val executedReadsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_replica_executed_reads_total")
    .help("Total number of reads that have been executed.")
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

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // ProxyReplica channels.
  private val proxyReplicas: Seq[Chan[ProxyReplica[Transport]]] =
    for (a <- config.proxyReplicaAddresses)
      yield chan[ProxyReplica[Transport]](a, ProxyReplica.serializer)

  // Replica index.
  logger.check(config.replicaAddresses.contains(address))
  private val index = config.replicaAddresses.indexOf(address)

  // The log of commands. We implement the log as a BufferMap as opposed to
  // something like a SortedMap for efficiency. `log` is public for testing.
  @JSExport
  val log =
    new util.BufferMap[CommandBatchOrNoop](options.logGrowSize)

  // In Evelyn Paxos, a client can issue a read directly to a replica. The read
  // is associated with a slot i. The replica must wait until log entry i has
  // been executed before executing the read. If a replica receives the read
  // request before i has been executed, then it stores the read request in
  // this log for later.
  @JSExportAll
  case class DeferredRead(
      command: Command,
      startTimeNanos: Long
  )

  @JSExport
  protected val deferredReads =
    new util.BufferMap[mutable.Buffer[DeferredRead]](options.logGrowSize)

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
            val recover = Recover(slot = executedWatermark)
            getProxyReplica() match {
              case Some(proxyReplica) =>
                proxyReplica.send(ProxyReplicaInbound().withRecover(recover))
              case None =>
                leaders.foreach(_.send(LeaderInbound().withRecover(recover)))
            }
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

  private def getProxyReplica(): Option[Chan[ProxyReplica[Transport]]] = {
    if (proxyReplicas.isEmpty) {
      return None
    }

    config.distributionScheme match {
      case Hash      => Some(proxyReplicas(rand.nextInt(proxyReplicas.size)))
      case Colocated => Some(proxyReplicas(index))
    }
  }

  private def clientChan(commandId: CommandId): Chan[Client[Transport]] = {
    val clientAddress = transport.addressSerializer.fromBytes(
      commandId.clientAddress.toByteArray()
    )
    chan[Client[Transport]](clientAddress, Client.serializer)
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
          if (slot % config.numReplicas == index) {
            clientReplies += ClientReply(commandId = commandId,
                                         slot = slot,
                                         result = result)
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

  // Executed any deferred reads in slot `slot`.
  //
  // TODO(mwhittaker): We can garbage collect reads if we want.
  private def processDeferredReads(reads: Seq[DeferredRead]): Unit = {
    // If we only have one read to perform, we send the result directly back to
    // the client. Otherwise, we send a batch of reads to the proxy replica (if
    // there are any).
    (reads.size == 1, getProxyReplica()) match {
      case (true, _) | (false, None) =>
        for (read <- reads) {
          val client = clientChan(read.command.commandId)
          client.send(ClientInbound().withReadReply(executeRead(read.command)))
          metrics.deferredReadLatency.observe(
            (System.nanoTime - read.startTimeNanos).toDouble / 1000000
          )
        }

      case (false, Some(proxyReplica)) =>
        val readReplies = mutable.Buffer[ReadReply]()
        for (read <- reads) {
          readReplies += executeRead(read.command)
          metrics.deferredReadLatency.observe(
            (System.nanoTime - read.startTimeNanos).toDouble / 1000000
          )
        }
        proxyReplica.send(
          ProxyReplicaInbound()
            .withReadReplyBatch(ReadReplyBatch(batch = readReplies))
        )
    }
  }

  private def executeLog(): ClientReplyBatch = {
    val clientReplies = mutable.Buffer[ClientReply]()

    while (true) {
      log.get(executedWatermark) match {
        case None =>
          // We have to execute the log in prefix order, so if we hit an empty
          // slot, we have to stop executing.
          return ClientReplyBatch(batch = clientReplies.toSeq)

        case Some(commandBatchOrNoop) =>
          val slot = executedWatermark
          executeCommandBatchOrNoop(slot, commandBatchOrNoop, clientReplies)
          deferredReads.get(slot) match {
            case None =>
            case Some(reads) =>
              processDeferredReads(reads)
              metrics.deferredReadBatchSize.observe(reads.size)
          }
          executedWatermark += 1

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
            getProxyReplica() match {
              case Some(proxyReplica) =>
                proxyReplica.send(
                  ProxyReplicaInbound().withChosenWatermark(
                    ChosenWatermark(slot = executedWatermark)
                  )
                )
              case None =>
                leaders.foreach(
                  _.send(
                    LeaderInbound().withChosenWatermark(
                      ChosenWatermark(slot = executedWatermark)
                    )
                  )
                )
            }
            metrics.chosenWatermarksSentTotal.inc()
          }
      }
    }

    // The above for loop will always return a ClientReplyBatch, but Scala
    // cannot figure that out automatically, so we need to raise an exception
    // here to placate the type checker.
    throw new IllegalStateException()
  }

  private def handleDeferrableRead(
      src: Transport#Address,
      slot: Int,
      command: Command
  ): Unit = {
    // We have to wait for `slot` to be executed before we execute the read. If
    // `slot >= executedWatermark`, then the slot hasn't yet been executed and
    // we have to defer the read to later.
    if (slot >= executedWatermark) {
      val read =
        DeferredRead(command = command, startTimeNanos = System.nanoTime)
      deferredReads.get(slot) match {
        case None             => deferredReads.put(slot, mutable.Buffer(read))
        case Some(otherReads) => otherReads += read
      }
      metrics.deferredReadsTotal.inc()
      return
    }

    val client = chan[Client[Transport]](src, Client.serializer)
    client.send(ClientInbound().withReadReply(executeRead(command)))
  }

  private def handleDeferrableReads(slot: Int, commands: Seq[Command]): Unit = {
    // We have to wait for `slot` to be executed before we execute the read. If
    // `slot >= executedWatermark`, then the slot hasn't yet been executed and
    // we have to defer the read to later.
    if (slot >= executedWatermark) {
      val startTimeNanos = System.nanoTime
      val reads = commands.map(
        command =>
          DeferredRead(command = command, startTimeNanos = startTimeNanos)
      )
      deferredReads.get(slot) match {
        case None             => deferredReads.put(slot, reads.toBuffer)
        case Some(otherReads) => otherReads ++= reads
      }
      metrics.deferredReadsTotal.inc()
      return
    }

    getProxyReplica() match {
      case Some(proxyReplica) =>
        proxyReplica.send(
          ProxyReplicaInbound().withReadReplyBatch(
            ReadReplyBatch(batch = commands.map(executeRead))
          )
        )

      case None =>
        for (command <- commands) {
          val client = clientChan(command.commandId)
          client.send(ClientInbound().withReadReply(executeRead(command)))
        }
    }
  }

  // Execute the read `command` on the current state of the state machine.
  private def executeRead(command: Command): ReadReply = {
    val result = timed("executeRead/execute read") {
      ByteString.copyFrom(
        stateMachine.run(command.command.toByteArray())
      )
    }
    metrics.executedReadsTotal.inc()

    ReadReply(
      commandId = command.commandId,
      // The `- 1` here is needed because of a difference in slot vs
      // watermark. If the watermark is 0, then we have executed only slot
      // -1.
      slot = executedWatermark - 1,
      result = result
    )
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ReplicaInbound.Request

    val label =
      inbound.request match {
        case Request.Chosen(_)                => "Chosen"
        case Request.ReadRequest(_)           => "ReadRequest"
        case Request.SequentialReadRequest(_) => "SequentialReadRequest"
        case Request.EventualReadRequest(_)   => "EventualReadRequest"
        case Request.ReadRequestBatch(_)      => "ReadRequestBatch"
        case Request.SequentialReadRequestBatch(_) =>
          "SequentialReadRequestBatch"
        case Request.EventualReadRequestBatch(_) => "EventualReadRequestBatch"
        case Request.Empty =>
          logger.fatal("Empty ReplicaInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Chosen(r) =>
          handleChosen(src, r)
        case Request.ReadRequest(r) =>
          handleReadRequest(src, r)
        case Request.SequentialReadRequest(r) =>
          handleSequentialReadRequest(src, r)
        case Request.EventualReadRequest(r) =>
          handleEventualReadRequest(src, r)
        case Request.ReadRequestBatch(r) =>
          handleReadRequestBatch(src, r)
        case Request.SequentialReadRequestBatch(r) =>
          handleSequentialReadRequestBatch(src, r)
        case Request.EventualReadRequestBatch(r) =>
          handleEventualReadRequestBatch(src, r)
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

    // If we have client replies, send them to a proxy replica (if there are
    // any).
    if (clientReplyBatch.batch.size > 0) {
      getProxyReplica() match {
        case Some(proxyReplica) =>
          proxyReplica.send(
            ProxyReplicaInbound().withClientReplyBatch(clientReplyBatch)
          )

        case None =>
          for (reply <- clientReplyBatch.batch) {
            val client = clientChan(reply.commandId)
            client.send(ClientInbound().withClientReply(reply))
          }
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

  private def handleReadRequest(
      src: Transport#Address,
      readRequest: ReadRequest
  ): Unit = {
    handleDeferrableRead(src, readRequest.slot, readRequest.command)
  }

  private def handleSequentialReadRequest(
      src: Transport#Address,
      sequentialReadRequest: SequentialReadRequest
  ): Unit = {
    handleDeferrableRead(src,
                         sequentialReadRequest.slot,
                         sequentialReadRequest.command)
  }

  private def handleEventualReadRequest(
      src: Transport#Address,
      eventualReadRequest: EventualReadRequest
  ): Unit = {
    val client = chan[Client[Transport]](src, Client.serializer)
    client.send(
      ClientInbound()
        .withReadReply(executeRead(eventualReadRequest.command))
    )
  }

  private def handleReadRequestBatch(
      src: Transport#Address,
      readRequestBatch: ReadRequestBatch
  ): Unit = {
    handleDeferrableReads(readRequestBatch.slot, readRequestBatch.command)
  }

  private def handleSequentialReadRequestBatch(
      src: Transport#Address,
      sequentialReadRequestBatch: SequentialReadRequestBatch
  ): Unit = {
    handleDeferrableReads(sequentialReadRequestBatch.slot,
                          sequentialReadRequestBatch.command)
  }

  private def handleEventualReadRequestBatch(
      src: Transport#Address,
      eventualReadRequestBatch: EventualReadRequestBatch
  ): Unit = {
    val results = timed("handleEventualReadBatch/execute read") {
      eventualReadRequestBatch.command.map(executeRead)
    }
    getProxyReplica() match {
      case Some(proxyReplica) =>
        proxyReplica.send(
          ProxyReplicaInbound()
            .withReadReplyBatch(ReadReplyBatch(batch = results))
        )
      case None =>
        for (readReply <- results) {
          val client = clientChan(readReply.commandId)
          client.send(ClientInbound().withReadReply(readReply))
        }
    }
  }
}
