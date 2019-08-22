package frankenpaxos.simplebpaxos

import VertexIdHelpers.vertexIdOrdering
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.clienttable.ClientTable
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
    // If a replica commits a vertex v that depends on uncommitted vertex u,
    // the replica will eventually recover u to ensure that v will eventually
    // be executed. The time that the replica waits before recovering u is
    // drawn uniformly at random between recoverVertexTimerMinPeriod and
    // recoverVertexTimerMaxPeriod.
    recoverVertexTimerMinPeriod: java.time.Duration,
    recoverVertexTimerMaxPeriod: java.time.Duration,
    // See frankenpaxos.epaxos.Replica for information on the following options.
    unsafeSkipGraphExecution: Boolean,
    executeGraphBatchSize: Int,
    executeGraphTimerPeriod: java.time.Duration
)

@JSExportAll
object ReplicaOptions {
  val default = ReplicaOptions(
    recoverVertexTimerMinPeriod = java.time.Duration.ofMillis(500),
    recoverVertexTimerMaxPeriod = java.time.Duration.ofMillis(1500),
    unsafeSkipGraphExecution = false,
    executeGraphBatchSize = 1,
    executeGraphTimerPeriod = java.time.Duration.ofSeconds(1)
  )
}

@JSExportAll
class ReplicaMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val executeGraphTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_replica_execute_graph_total")
    .help("Total number of times the replica executed the dependency graph.")
    .register()

  val executeGraphTimerTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_replica_execute_graph_timer_total")
    .help(
      "Total number of times the replica executed the dependency graph from " +
        "a timer."
    )
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_replica_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_replica_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_replica_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val recoverVertexTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_replica_recover_vertex_total")
    .help("Total number of times the replica recovered an instance.")
    .register()

  val dependencyGraphNumVertices: Gauge = collectors.gauge
    .build()
    .name("simple_bpaxos_replica_dependency_graph_num_vertices")
    .help("The number of vertices in the dependency graph.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_replica_dependencies")
    .help("The number of dependencies that a command has.")
    .register()
}

@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer

  type ClientPseudonym = Int
  type DepServiceNodeIndex = Int

  @JSExportAll
  case class Committed(
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId]
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

  // Channels to the proposers.
  private val proposers: Seq[Chan[Proposer[Transport]]] =
    for (a <- config.proposerAddresses)
      yield chan[Proposer[Transport]](a, Proposer.serializer)

  // The number of committed commands that are in the graph that have not yet
  // been processed. We process the graph every `options.executeGraphBatchSize`
  // committed commands and every `options.executeGraphTimerPeriod` seconds. If
  // the timer expires, we clear this number.
  @JSExportAll
  protected var numPendingCommittedCommands: Int = 0

  // The committed commands.
  val commands = mutable.Map[VertexId, Committed]()

  // The client table, which records the latest commands for each client.
  @JSExport
  protected val clientTable =
    new ClientTable[(Transport#Address, ClientPseudonym), Array[Byte]]()

  // If a replica commits a command in vertex A with a dependency on uncommitted
  // vertex B, then the replica sets a timer to recover vertex B. This prevents
  // a vertex from being forever stalled.
  @JSExport
  protected val recoverVertexTimers = mutable.Map[VertexId, Transport#Timer]()

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
          execute()
          numPendingCommittedCommands = 0
          t.start()
        }
      )
      t.start()
      Some(t)
    }

  // Helpers ///////////////////////////////////////////////////////////////////
  private def execute(): Unit = {
    val executable: Seq[VertexId] = dependencyGraph.execute()
    metrics.executeGraphTotal.inc()
    metrics.dependencyGraphNumVertices.set(dependencyGraph.numVertices)

    for (v <- executable) {
      import CommandOrNoop.Value
      commands.get(v) match {
        case None =>
          logger.fatal(
            s"Vertex $v is ready for execution but the replica doesn't have " +
              s"a Committed entry for it."
          )

        case Some(committed: Committed) =>
          executeCommand(v, committed.commandOrNoop)
      }
    }
  }

  private def executeCommand(
      vertexId: VertexId,
      commandOrNoop: CommandOrNoop
  ): Unit = {
    import CommandOrNoop.Value

    commandOrNoop.value match {
      case Value.Empty =>
        logger.fatal("Empty CommandOrNoop.")

      case Value.Noop(Noop()) =>
        metrics.executedNoopsTotal.inc()

      case Value.Command(command) =>
        val clientAddress = transport.addressSerializer.fromBytes(
          command.clientAddress.toByteArray
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
                s"already committed."
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

    val startNanos = System.nanoTime
    val label = inbound.request match {
      case Request.Commit(r) =>
        handleCommit(src, r)
        "Commit"
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
    val stopNanos = System.nanoTime
    metrics.requestsTotal.labels(label).inc()
    metrics.requestsLatency
      .labels(label)
      .observe((stopNanos - startNanos).toDouble / 1000000)
  }

  // Public for testing.
  def handleCommit(
      src: Transport#Address,
      commit: Commit
  ): Unit = {
    // If we've already recorded this command as committed, don't record it as
    // committed again.
    if (commands.contains(commit.vertexId)) {
      return
    }

    // Record the committed command.
    val dependencies = commit.dependency.toSet
    commands(commit.vertexId) = Committed(commit.commandOrNoop, dependencies)
    metrics.dependencies.observe(dependencies.size)

    // Stop any recovery timer for the current vertex, and start recovery
    // timers for any uncommitted vertices on which we depend.
    recoverVertexTimers.get(commit.vertexId).foreach(_.stop())
    recoverVertexTimers -= commit.vertexId
    for {
      v <- dependencies
      if !commands.contains(v)
      if !recoverVertexTimers.contains(v)
    } {
      recoverVertexTimers(v) = makeRecoverVertexTimer(v)
    }

    // If we're skipping the graph, execute the command right away. Otherwise,
    // commit the command to the dependency graph and execute the graph if we
    // have sufficiently many commands pending execution.
    if (options.unsafeSkipGraphExecution) {
      executeCommand(commit.vertexId, commit.commandOrNoop)
    } else {
      dependencyGraph.commit(
        commit.vertexId,
        (),
        VertexIdPrefixSet(config.leaderAddresses.size, dependencies)
      )
      numPendingCommittedCommands += 1
      if (numPendingCommittedCommands % options.executeGraphBatchSize == 0) {
        execute()
        numPendingCommittedCommands = 0
        executeGraphTimer.foreach(_.reset())
      }
    }
  }
}
