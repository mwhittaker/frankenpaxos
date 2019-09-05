package frankenpaxos.simplegcbpaxos

import VertexIdHelpers.vertexIdOrdering
import com.google.protobuf.ByteString
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Serializer
import frankenpaxos.Util
import frankenpaxos.clienttable.ClientTable
import frankenpaxos.clienttable.ClientTableProto
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.statemachine.StateMachine
import scala.collection.mutable
import scala.concurrent.Future
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
case class ReplicaOptions(
    // `commands` BufferMap grow size.
    commandsGrowSize: Int,
    // If a replica commits a vertex v that depends on uncommitted vertex u,
    // the replica will eventually recover u to ensure that v will eventually
    // be executed. The time that the replica waits before recovering u is
    // drawn uniformly at random between recoverVertexTimerMinPeriod and
    // recoverVertexTimerMaxPeriod.
    recoverVertexTimerMinPeriod: java.time.Duration,
    recoverVertexTimerMaxPeriod: java.time.Duration,
    // If `unsafeDontRecover` is true, replicas don't make any attempt to
    // recover vertices. This is not live and should only be used for
    // performance debugging.
    unsafeDontRecover: Boolean,
    // See frankenpaxos.epaxos.Replica for information on the following options.
    executeGraphBatchSize: Int,
    executeGraphTimerPeriod: java.time.Duration,
    unsafeSkipGraphExecution: Boolean,
    // numBlockers argument to DependencyGraph#execute. -1 is None.
    numBlockers: Int,
    // A replica sends a GarbageCollect message to the proposers and acceptors
    // every `sendWatermarkEveryNCommands` commands that it receives.
    sendWatermarkEveryNCommands: Int,
    // We want replicas to send a snapshot command every
    // `sendWatermarkEveryNCommands` commands. Replicas take turn sending the
    // commands, so if there are r replicas, then they send every `r *
    // sendSnapshotEveryNCommands` commands with offsets.
    sendSnapshotEveryNCommands: Int,
    // If true, the replica records how long various things take to do and
    // reports them using the `simple_gc_bpaxos_replica_requests_latency` metric.
    measureLatencies: Boolean
)

@JSExportAll
object ReplicaOptions {
  val default = ReplicaOptions(
    commandsGrowSize = 5000,
    recoverVertexTimerMinPeriod = java.time.Duration.ofMillis(500),
    recoverVertexTimerMaxPeriod = java.time.Duration.ofMillis(1500),
    unsafeDontRecover = false,
    executeGraphBatchSize = 1,
    executeGraphTimerPeriod = java.time.Duration.ofSeconds(1),
    unsafeSkipGraphExecution = false,
    numBlockers = -1,
    sendWatermarkEveryNCommands = 10000,
    sendSnapshotEveryNCommands = 10000,
    measureLatencies = true
  )
}

@JSExportAll
class ReplicaMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val executeGraphTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_execute_graph_total")
    .help("Total number of times the replica executed the dependency graph.")
    .register()

  val executeGraphTimerTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_execute_graph_timer_total")
    .help(
      "Total number of times the replica executed the dependency graph from " +
        "a timer."
    )
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val executedSnapshotsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_executed_snapshots_total")
    .help("Total number of \"executed\" snapshots.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val recoverVertexTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_recover_vertex_total")
    .help("Total number of times the replica recovered an instance.")
    .register()

  val dependencyGraphNumVertices: Gauge = collectors.gauge
    .build()
    .name("simple_gc_bpaxos_replica_dependency_graph_num_vertices")
    .help("The number of vertices in the dependency graph.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_replica_dependencies")
    .help("The number of dependencies that a command has.")
    .register()

  val uncompactedDependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_replica_uncompacted_dependencies")
    .help("The number of uncompacted dependencies that a command has.")
    .register()

  val uncommittedDependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_replica_uncommitted_dependencies")
    .help("The number of uncommitted dependencies that a command has.")
    .register()

  val timerDependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_replica_timer_dependencies")
    .help("The number of timer dependencies that a command has.")
    .register()

  val recoverVertexTimers: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_recover_vertex_timers")
    .help("The number of recover vertex timers that a replica has.")
    .register()

  val ignoredCommitSnapshotsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_replica_ignored_commit_snapshots_total")
    .help(
      "Total number of times the replica ignored a CommitSnapshot request " +
        "because it had a more recent snapshot."
    )
    .register()
}

@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer

  type ClientPseudonym = Int

  @JSExportAll
  case class Committed(
      proposal: Proposal,
      dependencies: VertexIdPrefixSet
  )

  // TODO(mwhittaker): Some things here are clones (watermark) and some are
  // already serialized (stateMachine and clientTable). Figure out which is
  // best for each field. It should depend on the cost of serialization vs
  // cloning.
  @JSExportAll
  case class Snapshot(
      id: Int,
      watermark: VertexIdPrefixSet,
      stateMachine: Array[Byte],
      clientTable: ClientTableProto
  )
}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    // Public for Javascript visualizations.
    val stateMachine: StateMachine,
    // Public for Javascript visualizations.
    val dependencyGraph: DependencyGraph[VertexId, Unit, VertexIdPrefixSet],
    options: ReplicaOptions = ReplicaOptions.default,
    metrics: ReplicaMetrics = new ReplicaMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  import Replica._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and get our index.
  logger.check(config.valid())
  logger.check(config.replicaAddresses.contains(address))
  private val index = config.replicaAddresses.indexOf(address)

  // Random number generator.
  val rand = new Random(seed)

  // Channels to the co-located garbage collector.
  private val garbageCollector: Chan[GarbageCollector[Transport]] =
    chan[GarbageCollector[Transport]](config.garbageCollectorAddresses(index),
                                      GarbageCollector.serializer)

  // Channels to the leaders.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Channels to the proposers.
  private val proposers: Seq[Chan[Proposer[Transport]]] =
    for (a <- config.proposerAddresses)
      yield chan[Proposer[Transport]](a, Proposer.serializer)

  // Channels to the other replicas.
  private val otherReplicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses if a != address)
      yield chan[Replica[Transport]](a, Replica.serializer)

  // The number of committed commands that the replica has received since the
  // last time it sent a GarbageCollect message to the garbage collectors.
  // Every `options.sendWatermarkEveryNCommands` commands, this value is reset
  // and GarbageCollect messages are sent.
  @JSExport
  protected var numCommandsPendingWatermark: Int = 0

  // The number of committed commands that the replica has received since the
  // last time it sent a SnapshotRequest to a leader. Every `r *
  // options.sendSnapshotEveryNCommands` commands, this value is reset and a
  // SnapshotRequest is sent.
  @JSExport
  protected var numCommandsPendingSendSnapshot
    : Int = options.sendSnapshotEveryNCommands * index

  // The number of committed commands that are in the graph that have not yet
  // been processed. We process the graph every `options.executeGraphBatchSize`
  // committed commands and every `options.executeGraphTimerPeriod` seconds. If
  // the timer expires, we clear this number.
  @JSExport
  protected var numCommandsPendingExecution: Int = 0

  // The committed commands. Recall that logically, commands forms a
  // two-dimensional array indexed by leader index and id.
  //
  //            .   .   .
  //            .   .   .
  //            .   .   .
  //          +---+---+---+
  //        3 |   |   | f |
  //          +---+---+---+
  //        2 |   | c | e |
  //     i    +---+---+---+
  //     d  1 | a |   |   |
  //          +---+---+---+
  //        0 | b |   | d |
  //          +---+---+---+
  //            0   1   2
  //          leader index
  //
  // We represent this log as a map, where commands[VertexId(leaderIndex, id)]
  // stores entry found in row id and column leaderIndex in the array.
  val commands = new VertexIdBufferMap[Committed](
    numLeaders = config.leaderAddresses.size,
    growSize = options.commandsGrowSize
  )

  // `committedVertices` stores the set of vertex ids that
  //
  //   (1) appear in `commands`,
  //   (2) were garbage collected from `commands`, or
  //   (3) were executed as part of the snapshot in `snapshot`.
  //
  // This includes _all_ commands, including noops and snapshots. For example,
  // imagine we have the following `commands`:
  //
  //          +---+---+---+
  //        3 |   |   |   |
  //          +---+---+---+
  //        2 |   |   | c |
  //     i    +---+---+---+
  //     d  1 | a | # |   |
  //          +---+---+---+
  //        0 | # | # | b |
  //          +---+---+---+
  //            0   1   2
  //          leader index
  //
  // and a snapshot that includes commands (0, 0), (1, 0), (1, 1), and (2, 1).
  // Then
  //
  //   - (0, 1), (2, 0), and (2, 2) appear in `committedVertices` because they
  //     appear in `commands`
  //   - (0, 0), (1, 0), and (1, 1) appear in `committedVertices` because they
  //     were garbage collected.
  //   - (0, 0), (1, 0), and (1, 1), and (2, 1) appear in `committedVertices`
  //     because they are in the most recent snapshot.
  //
  // Note that (2, 1) is a hole in `commands`. This is okay. There is no hole
  // in `committedVertices`, and as long as `committedVertices` doesn't have
  // holes, `commands` will continue getting garbage collected.
  @JSExport
  protected val committedVertices = VertexIdPrefixSet(
    config.leaderAddresses.size
  )

  // `executedVertices` stores the set of vertex ids that have been executed by
  // this replica or have been executed as part of this replica's snapshot. For
  // example, if this replica executes commands a, b, and c and then receives a
  // snapshot with commands a, b, and d, then `executedVertices` would contain
  // vertices of commands a, b, c, and d. Note that this includes _all_
  // commands, including noops and snapshots.
  @JSExport
  protected val executedVertices = VertexIdPrefixSet(
    config.leaderAddresses.size
  )

  // The most recent snapshot.
  @JSExport
  protected var snapshot: Option[Snapshot] = None

  // `history` stores the set of vertex ids that have been executed on this
  // replica since the last snapshot was taken (either independently or from
  // another replica). Note that commands here include only commands, not noops
  // and snapshots. Those do not have to be repeated.
  //
  // Whenever a replica executes a command, it adds it to history. If it takes
  // a snapshot, then it clears history. If it receives a snapshot, then it
  // removes from history all commands in the snapshot.
  @JSExport
  protected var history = mutable.Buffer[VertexId]()

  // The client table, which records the latest commands for each client.
  implicit val addressSerializer = transport.addressSerializer
  @JSExport
  protected var clientTable =
    ClientTable[(Transport#Address, ClientPseudonym), Array[Byte]]()

  // The `numBlockers` argument passed to the dependency graph.
  private val numBlockers =
    if (options.numBlockers == -1) None else Some(options.numBlockers)

  // Timers to recover vertices. There are a number of different policies that
  // specify when we should try and recover a timer. For example, we may set a
  // recovery timer for a vertex as soon as we know it exits but is not
  // committed. Or, we may want to set timers for only a handful of vertices.
  // There are a lot of options. Whatever option we choose, the timers are
  // stored here.
  @JSExport
  protected val recoverVertexTimers = mutable.Map[VertexId, Transport#Timer]()
  metrics.recoverVertexTimers.observe(recoverVertexTimers.size)

  // A timer to execute the dependency graph. If the batch size is 1 or if
  // graph execution is disabled, then there is no need for the timer.
  @JSExport
  protected val executeGraphTimer: Option[Transport#Timer] =
    if (options.executeGraphBatchSize == 1 ||
        options.unsafeSkipGraphExecution) {
      None
    } else {
      lazy val t: Transport#Timer = timer(
        "executeGraphTimer",
        options.executeGraphTimerPeriod,
        () => {
          metrics.executeGraphTimerTotal.inc()
          timed("executeGraphTimer/execute") { execute() }
          numCommandsPendingExecution = 0
          t.start()
        }
      )
      t.start()
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

  // Add a committed command to `commands`. The reason this is pulled into a
  // helper function is to ensure that we don't forget to also update
  // `committedVertices`.
  private def recordCommitted(
      vertexId: VertexId,
      committed: Committed
  ): Unit = {
    commands.put(vertexId, committed)
    committedVertices.add(vertexId)
  }

  // `executables` and `blockers` are used by `execute`. Using and clearing a
  // field is a little more efficient than allocating a new set every time. It
  // also reduces memory pressure, allowing us to garbage collect less often.
  private val executables = mutable.Buffer[VertexId]()
  private val blockers = mutable.Set[VertexId]()

  private def execute(): Unit = {
    // Collect executable commands and blockers.
    timed("execute/dependencyGraph.appendExecute") {
      dependencyGraph.appendExecute(numBlockers, executables, blockers)
    }
    metrics.executeGraphTotal.inc()
    metrics.dependencyGraphNumVertices.set(dependencyGraph.numVertices)

    // Register recover timers for the blockers.
    if (options.unsafeDontRecover) {
      // Don't fuss with timers.
    } else {
      timed("execute/recoverVertexTimers") {
        for (v <- blockers) {
          if (!recoverVertexTimers.contains(v)) {
            recoverVertexTimers(v) = makeRecoverVertexTimer(v)
          }
        }
      }
      metrics.recoverVertexTimers.observe(recoverVertexTimers.size)
    }

    // Execute the executables.
    timed("execute/execute") {
      for (v <- executables) {
        import Proposal.Value
        commands.get(v) match {
          case None =>
            logger.fatal(
              s"Vertex $v is ready for execution but the replica doesn't " +
                s"have a Committed entry for it."
            )

          case Some(committed: Committed) =>
            executeProposal(v, committed.proposal)
        }
      }
    }

    // Clear the fields.
    executables.clear()
    blockers.clear()
  }

  private def executeProposal(
      vertexId: VertexId,
      proposal: Proposal
  ): Unit = {
    executedVertices.add(vertexId)

    import Proposal.Value
    proposal.value match {
      case Value.Empty =>
        logger.fatal("Empty Proposal.")

      case Value.Noop(Noop()) =>
        metrics.executedNoopsTotal.inc()

      case Value.Snapshot(_) =>
        // Record the snapshot.
        snapshot = timed("Proposal/takeSnapshot") {
          Some(
            Snapshot(id = snapshot.map(_.id + 1).getOrElse(0),
                     watermark = executedVertices.clone(),
                     stateMachine = stateMachine.toBytes(),
                     clientTable = clientTable.toProto())
          )
        }
        metrics.executedSnapshotsTotal.inc()

        // We clear our history of executed commands when we execute a
        // snapshot. Only unsnapshotted commands are stored in history.
        history.clear()

        // Garbage commands that are below the snapshot's watermark. Some
        // executed commands might remain, but that's ok. They'll get garbage
        // collected eventually.
        timed("Proposal/garbageCollectCommands") {
          commands.garbageCollect(executedVertices.getWatermark())
        }

      case Value.Command(command) =>
        timed("executeProposal/Command") {
          val clientAddress = transport.addressSerializer.fromByteString(
            command.clientAddress
          )
          val clientIdentity = (clientAddress, command.clientPseudonym)
          clientTable.executed(clientIdentity, command.clientId) match {
            case ClientTable.Executed(None) =>
              // Don't execute the same command twice.
              metrics.repeatedCommandsTotal.inc()

            case ClientTable.Executed(Some(output)) =>
              // Don't execute the same command twice. Also, replay the output
              // to the client.
              metrics.repeatedCommandsTotal.inc()
              val client =
                chan[Client[Transport]](clientAddress, Client.serializer)
              client.send(
                ClientInbound().withClientReply(
                  ClientReply(clientPseudonym = command.clientPseudonym,
                              clientId = command.clientId,
                              result = ByteString.copyFrom(output))
                )
              )

            case ClientTable.NotExecuted =>
              val output = stateMachine.run(command.command.toByteArray)
              clientTable.execute(clientIdentity, command.clientId, output)
              history += vertexId
              metrics.executedCommandsTotal.inc()

              // TODO(mwhittaker): Think harder about if this is live.
              if (index == vertexId.leaderIndex % config.replicaAddresses.size) {
                val client =
                  chan[Client[Transport]](clientAddress, Client.serializer)
                client.send(
                  ClientInbound().withClientReply(
                    ClientReply(clientPseudonym = command.clientPseudonym,
                                clientId = command.clientId,
                                result = ByteString.copyFrom(output))
                  )
                )
              }
          }
        }
    }
  }

  // Send GarbageCollect messages if needed.
  private def sendWatermarkIfNeeded(): Unit = {
    numCommandsPendingWatermark += 1
    if (numCommandsPendingWatermark % options.sendWatermarkEveryNCommands == 0) {
      garbageCollector.send(
        GarbageCollectorInbound().withGarbageCollect(
          GarbageCollect(replicaIndex = index,
                         frontier = committedVertices.getWatermark())
        )
      )
      numCommandsPendingWatermark = 0
    }
  }

  // Send SnapshotRequest if needed.
  private def sendSnapshotIfNeeded(): Unit = {
    numCommandsPendingSendSnapshot += 1
    val n = options.sendSnapshotEveryNCommands * config.replicaAddresses.size
    if (numCommandsPendingSendSnapshot % n == 0) {
      val leader = leaders(rand.nextInt(leaders.size))
      leader.send(LeaderInbound().withSnapshotRequest(SnapshotRequest()))
      numCommandsPendingSendSnapshot = 0
    }
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeRecoverVertexTimer(vertexId: VertexId): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"recoverVertex [$vertexId]",
      Util.randomDuration(options.recoverVertexTimerMinPeriod,
                          options.recoverVertexTimerMaxPeriod),
      () => {
        metrics.recoverVertexTotal.inc()

        // Sanity check.
        commands.get(vertexId) match {
          case Some(_) =>
            logger.fatal(
              s"Replica recovering vertex $vertexId, but that vertex is " +
                s"already committed.\ncommands = $commands\n" +
                s"recoverVertexTimers = $recoverVertexTimers"
            )
          case None =>
        }

        // Send a recover message to a randomly selected leader. We randomly
        // select a leader to avoid dueling leaders.
        //
        // TODO(mwhittaker): Send the recover message intially to the proposer
        // that led the instance. Only if that proposer is dead should we send
        // to another proposer.
        val proposer = proposers(rand.nextInt(proposers.size))
        proposer.send(
          ProposerInbound().withRecover(
            Recover(vertexId = vertexId)
          )
        )

        // We also send recovery messages to all other replicas. If proposers
        // have garbage collected the vertex that we're trying to recover,
        // they'll ignore us, so it's up to us to contact the replicas.
        otherReplicas.foreach(
          _.send(ReplicaInbound().withRecover(Recover(vertexId = vertexId)))
        )

        t.start()
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    import ReplicaInbound.Request

    val label = inbound.request match {
      case Request.Commit(_)         => "Commit"
      case Request.Recover(_)        => "Recover"
      case Request.CommitSnapshot(_) => "CommitSnapshot"
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Commit(r)         => handleCommit(src, r)
        case Request.Recover(r)        => handleRecover(src, r)
        case Request.CommitSnapshot(r) => handleCommitSnapshot(src, r)
        case Request.Empty => {
          logger.fatal("Empty ReplicaInbound encountered.")
        }
      }
    }
  }

  private def handleCommit(
      src: Transport#Address,
      commit: Commit
  ): Unit = {
    // If we've already recorded this command as committed, don't record it as
    // committed again. Note that because of snapshots, the command may
    // actually be missing from `commands`, but still marked as committed.
    if (committedVertices.contains(commit.vertexId)) {
      logger.debug(
        s"Replica received command ${commit.vertexId} but has already " +
          s"committed the vertex. It is ignoring the commit."
      )
      return
    }

    // Record the committed command.
    val dependencies = timed("Commit/parseDependencies") {
      VertexIdPrefixSet.fromProto(commit.dependencies)
    }
    timed("Commit/recordCommitted") {
      recordCommitted(commit.vertexId, Committed(commit.proposal, dependencies))
    }
    metrics.dependencies.observe(dependencies.size)
    metrics.uncompactedDependencies.observe(dependencies.uncompactedSize)

    // Stop any recovery timer for the current vertex.
    if (options.unsafeDontRecover) {
      // Don't fuss with timers.
    } else {
      recoverVertexTimers.get(commit.vertexId) match {
        case Some(timer) =>
          timer.stop()
          recoverVertexTimers -= commit.vertexId
        case None =>
        // Do nothing.
      }
    }

    // If we're skipping the graph, execute the command right away. Otherwise,
    // commit the command to the dependency graph and execute the graph if we
    // have sufficiently many commands pending execution.
    if (options.unsafeSkipGraphExecution) {
      timed("Commit/unsafeExecuteCommand") {
        executeProposal(commit.vertexId, commit.proposal)
      }
    } else {
      timed("Commit/dependencyGraphCommit") {
        dependencyGraph.commit(commit.vertexId, (), dependencies)
      }
      numCommandsPendingExecution += 1
      if (numCommandsPendingExecution % options.executeGraphBatchSize == 0) {
        timed("Commit/execute") { execute() }
        numCommandsPendingExecution = 0
        executeGraphTimer.foreach(_.reset())
      }
    }

    sendWatermarkIfNeeded()
    sendSnapshotIfNeeded()
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    // If the vertex being recovered has already been garbage collected, then
    // we send back our snapshot. The snapshot contains the vertex.
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    snapshot match {
      case None =>
      // Nothing to do.

      case Some(snapshot) =>
        if (snapshot.watermark.contains(recover.vertexId)) {
          replica.send(
            ReplicaInbound().withCommitSnapshot(
              CommitSnapshot(
                id = snapshot.id,
                watermark = snapshot.watermark.toProto(),
                stateMachine = ByteString.copyFrom(snapshot.stateMachine),
                clientTable = snapshot.clientTable
              )
            )
          )
          return
        }
    }

    // If the command has not been garbage collected, then we either have it or
    // dont'.
    commands.get(recover.vertexId) match {
      case None =>
      // If we don't have the vertex, we simply ignore the message.

      case Some(committed: Committed) =>
        // If we do have the vertex, then we return it.
        replica.send(
          ReplicaInbound().withCommit(
            Commit(vertexId = recover.vertexId,
                   proposal = committed.proposal,
                   dependencies = committed.dependencies.toProto)
          )
        )
    }
  }

  private def handleCommitSnapshot(
      src: Transport#Address,
      commitSnapshot: CommitSnapshot
  ): Unit = {
    // Whether to replace our snapshot. We have to be careful not to replace an
    // older snapshot with a previous snapshot.
    val replaceSnapshot = snapshot match {
      // If we don't have a snapshot yet, then we are safe to replace it.
      case None => true
      // Otherwise, we only replace our snapshot if the new snapshot has a
      // higher snapshot id than ours.
      case Some(snapshot) => commitSnapshot.id > snapshot.id
    }

    if (!replaceSnapshot) {
      metrics.ignoredCommitSnapshotsTotal.inc()
      return
    }

    // Update our state.
    stateMachine.fromBytes(commitSnapshot.stateMachine.toByteArray)
    clientTable = ClientTable.fromProto(commitSnapshot.clientTable)
    val watermark = VertexIdPrefixSet.fromProto(commitSnapshot.watermark)
    commands.garbageCollect(watermark.getWatermark())
    committedVertices.addAll(watermark)
    executedVertices.addAll(watermark)

    // Update our snapshot.
    snapshot = Some(
      Snapshot(id = commitSnapshot.id,
               watermark = watermark,
               stateMachine = commitSnapshot.stateMachine.toByteArray,
               clientTable = commitSnapshot.clientTable)
    )

    // Stop and delete recovery timers for any vertices that we received as
    // part of the snapshot.
    recoverVertexTimers.retain({
      case (v, timer) =>
        val delete = watermark.contains(v)
        if (delete) {
          timer.stop()
        }
        delete
    })

    // Re-execute commands we had executed that were not part of the snapshot.
    // For example, imagine we had executed the following graph:
    //
    //     a <-- b
    //     ^     ^
    //     |     |
    //     c <-- d <-- e
    //           ^
    //           |
    //           f
    //
    // and we receive a snapshot with commands a, b, c, and d. We replace our
    // state with the snapshot, but we don't want to lose the effects of
    // executing e and f, so we re-execute them.
    timed("Commit/reexecuteHistory") {
      val newHistory = mutable.Buffer[VertexId]()
      for (vertexId <- history) {
        if (watermark.contains(vertexId)) {
          // Do _not_ execute commands that are part of the snapshot.
        } else {
          // Do execute commands that are _not_ part of the snapshot. If we've
          // executed a command and we haven't snapshotted it, then we know it's
          // in `commands`.
          val committed = commands.get(vertexId)
          logger.check(committed.isDefined)
          executeProposal(vertexId, committed.get.proposal)
          newHistory += vertexId
        }
      }
      history = newHistory
    }

    // Update the dependency graph with the newly executed commands. Check to
    // see if any commands are now eligible for execution. We don't bother
    // mucking with numCommandsPendingExecution or anything like that since
    // recovering from snapshots happens rarely.
    timed("CommitSnapshot/updateExecuted") {
      dependencyGraph.updateExecuted(watermark)
    }
    timed("CommitSnapshot/execute") {
      execute()
    }
  }
}
