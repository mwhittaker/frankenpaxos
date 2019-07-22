package frankenpaxos.epaxos

import InstanceHelpers.instanceOrdering
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.clienttable.ClientTable
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.TarjanDependencyGraph
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
    // If a replica commits a instance v that depends on uncommitted instance
    // u, the replica will eventually recover u to ensure that v will
    // eventually be executed. The time that the replica waits before
    // recovering u is drawn uniformly at random between
    // recoverInstanceTimerMinPeriod and recoverInstanceTimerMaxPeriod.
    recoverInstanceTimerMinPeriod: java.time.Duration,
    recoverInstanceTimerMaxPeriod: java.time.Duration,
    // If `unsafeSkipGraphExecution` is true, leaders skip graph execution
    // entirely. Instead, they execute commands as soon as they are committed.
    //
    // As the name suggests, this is not safe. It completely breaks the
    // protocol. This flag should only be used to debug performance issues. For
    // example, disabling graph execution makes it easier to see if graph
    // execution is a bottleneck.
    //
    // Note that if unsafeSkipGraphExecution is true, then
    // executeGraphBatchSize and executeGraphTimerPeriod are completely
    // ignored.
    unsafeSkipGraphExecution: Boolean,
    // When a leader receives a committed command, it adds it to its
    // dependency graph. When executeGraphBatchSize commands have been
    // committed, the leader attempts to execute as many commands in the graph
    // as possible. If executeGraphBatchSize commands have not been committed
    // within executeGraphTimerPeriod since the last time the graph was
    // executed, the graph is executed.
    executeGraphBatchSize: Int,
    executeGraphTimerPeriod: java.time.Duration
)

@JSExportAll
object ReplicaOptions {
  val default = ReplicaOptions(
    recoverInstanceTimerMinPeriod = java.time.Duration.ofMillis(500),
    recoverInstanceTimerMaxPeriod = java.time.Duration.ofMillis(1500),
    unsafeSkipGraphExecution = false,
    executeGraphBatchSize = 1,
    executeGraphTimerPeriod = java.time.Duration.ofSeconds(1)
  )
}

@JSExportAll
class ReplicaMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val executeGraphTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_execute_graph_total")
    .help("Total number of times the replica executed the dependency graph.")
    .register()

  val executeGraphTimerTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_execute_graph_timer_total")
    .help(
      "Total number of times the replica executed the dependency graph from " +
        "a timer."
    )
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val recoverInstanceTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_recover_instance_total")
    .help("Total number of times the replica recovered an instance.")
    .register()

  val dependencyGraphNumVertices: Gauge = collectors.gauge
    .build()
    .name("epaxos_replica_dependency_graph_num_vertices")
    .help("The number of vertices in the dependency graph.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("epaxos_replica_dependencies")
    .help("The number of dependencies that a command has.")
    .register()
}

@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer

  type ClientPseudonym = Int
  type DepServiceNodeIndex = Int
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
    val dependencyGraph: DependencyGraph[Instance, Unit] =
      new TarjanDependencyGraph(),
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

  // Channels to the leaders.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // The number of committed commands that are in the graph that have not yet
  // been processed. We process the graph every `options.executeGraphBatchSize`
  // committed commands and every `options.executeGraphTimerPeriod` seconds. If
  // the timer expires, we clear this number.
  @JSExportAll
  protected var numPendingCommittedCommands: Int = 0

  // The committed commands.
  val commands = mutable.Map[Instance, Leader.CommandTriple]()

  // The client table, which records the latest commands for each client.
  @JSExport
  protected val clientTable =
    new ClientTable[(Transport#Address, ClientPseudonym), Array[Byte]]()

  // If a replica commits a command in instance A with a dependency on
  // uncommitted instance B, then the replica sets a timer to recover instance
  // B. This prevents a instance from being forever stalled.
  @JSExport
  protected val recoverInstanceTimers = mutable.Map[Instance, Transport#Timer]()

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
    val executable: Seq[Instance] = dependencyGraph.execute()
    metrics.executeGraphTotal.inc()
    metrics.dependencyGraphNumVertices.set(dependencyGraph.numVertices)

    for (instance <- executable) {
      import CommandOrNoop.Value
      commands.get(instance) match {
        case None =>
          logger.fatal(
            s"Instance $instance is ready for execution but the replica " +
              s"doesn't have a committed entry for it."
          )

        case Some(triple: Leader.CommandTriple) =>
          executeTriple(instance, triple)
      }
    }
  }

  private def executeTriple(
      instance: Instance,
      triple: Leader.CommandTriple
  ): Unit = {
    import CommandOrNoop.Value

    triple.commandOrNoop.value match {
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

            // The leader of the command instance returns the response to the
            // client. If the leader is dead, then the client will eventually
            // re-send its request and some other leader will reply, either
            // from its client log or by getting the command chosen in a new
            // instance.
            if (index == instance.leaderIndex % config.replicaAddresses.size) {
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
  private def makeRecoverInstanceTimer(instance: Instance): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"recoverInstance [$instance]",
      Util.randomDuration(options.recoverInstanceTimerMinPeriod,
                          options.recoverInstanceTimerMaxPeriod),
      () => {
        metrics.recoverInstanceTotal.inc()

        // Sanity check.
        commands.get(instance) match {
          case Some(_) =>
            logger.fatal(
              s"Replica recovering instance $instance, but that instance is " +
                s"already committed."
            )
          case None =>
        }

        // Send a recover message to a randomly selected leader. We randomly
        // select a leader to avoid dueling leaders.
        //
        // TODO(mwhittaker): Send the recover message intially to the leader
        // that led the instance. Only if that leader is dead should we send
        // to another leader.
        val leader = leaders(rand.nextInt(leaders.size))
        leader.send(LeaderInbound().withRecover(Recover(instance = instance)))

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
    inbound.request match {
      case Request.Commit(r) =>
        metrics.requestsTotal.labels("Commit").inc()
        handleCommit(src, r)
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  // Public for testing.
  def handleCommit(
      src: Transport#Address,
      commit: Commit
  ): Unit = {
    // If we've already recorded this command as committed, don't record it as
    // committed again.
    if (commands.contains(commit.instance)) {
      return
    }

    // Record the committed command.
    val dependencies = commit.dependencies.toSet
    val triple = Leader.CommandTriple(commit.commandOrNoop,
                                      commit.sequenceNumber,
                                      dependencies)
    commands(commit.instance) = triple
    metrics.dependencies.observe(dependencies.size)

    // Stop any recovery timer for the current instance, and start recovery
    // timers for any uncommitted vertices on which we depend.
    recoverInstanceTimers.get(commit.instance).foreach(_.stop())
    recoverInstanceTimers -= commit.instance
    for {
      v <- dependencies
      if !commands.contains(v)
      if !recoverInstanceTimers.contains(v)
    } {
      recoverInstanceTimers(v) = makeRecoverInstanceTimer(v)
    }

    // If we're skipping the graph, execute the command right away. Otherwise,
    // commit the command to the dependency graph and execute the graph if we
    // have sufficiently many commands pending execution.
    if (options.unsafeSkipGraphExecution) {
      executeTriple(commit.instance, triple)
    } else {
      dependencyGraph.commit(commit.instance,
                             commit.sequenceNumber,
                             dependencies)
      numPendingCommittedCommands += 1
      if (numPendingCommittedCommands % options.executeGraphBatchSize == 0) {
        execute()
        numPendingCommittedCommands = 0
        executeGraphTimer.foreach(_.reset())
      }
    }
  }
}
