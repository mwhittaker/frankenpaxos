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
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.monitoring.Gauge
import frankenpaxos.statemachine.StateMachine
import scala.collection.mutable
import scala.concurrent.Future
import scala.scalajs.js.annotation._

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class LeaderOptions(
    resendDependencyRequestsTimerPeriod: java.time.Duration,
    recoverVertexTimerMinPeriod: java.time.Duration,
    recoverVertexTimerMaxPeriod: java.time.Duration,
    proposerOptions: ProposerOptions
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendDependencyRequestsTimerPeriod = java.time.Duration.ofSeconds(1),
    recoverVertexTimerMinPeriod = java.time.Duration.ofMillis(500),
    recoverVertexTimerMaxPeriod = java.time.Duration.ofMillis(1500),
    proposerOptions = ProposerOptions.default
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val committedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_committed_commands_total")
    .help(
      "Total number of commands that were committed (with potential " +
        "duplicates)."
    )
    .register()

  val resendDependencyRequestsTotalTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_resend_dependency_requests_total")
    .help("Total number of times the leader resent DependencyRequest messages.")
    .register()

  val dependencyGraphNumVertices: Gauge = collectors.gauge
    .build()
    .name("simple_bpaxos_leader_dependency_graph_num_vertices")
    .help("The number of vertices in the dependency graph.")
    .register()

  val dependencyGraphNumEdges: Gauge = collectors.gauge
    .build()
    .name("simple_bpaxos_leader_dependency_graph_num_edges")
    .help("The number of edges in the dependency graph.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_leader_dependencies")
    .help("The number of dependencies that a command has.")
    .register()
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer

  type ClientPseudonym = Int
  type DepServiceNodeIndex = Int

  @JSExportAll
  sealed trait State[Transport <: frankenpaxos.Transport[Transport]]

  @JSExportAll
  case class WaitingForDeps[Transport <: frankenpaxos.Transport[Transport]](
      command: Command,
      dependencyReplies: mutable.Map[DepServiceNodeIndex, DependencyReply],
      resendDependencyRequestsTimer: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class WaitingForConsensus[Transport <: frankenpaxos.Transport[Transport]](
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId],
      future: Future[Acceptor.VoteValue]
  ) extends State[Transport]

  // TODO(mwhittaker): Decide whether we need a Committed entry.
  @JSExportAll
  case class Committed[Transport <: frankenpaxos.Transport[Transport]](
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId]
  ) extends State[Transport]
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    // Public for Javascript visualizations.
    val stateMachine: StateMachine,
    // Public for Javascript visualizations.
    val dependencyGraph: DependencyGraph[VertexId, Unit] =
      new JgraphtDependencyGraph(),
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors),
    proposerMetrics: ProposerMetrics = new ProposerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Leader._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and get our index.
  logger.check(config.valid())
  logger.check(config.leaderAddresses.contains(address))
  private val index = config.leaderAddresses.indexOf(address)

  // Channels to the dependency service nodes.
  private val depServiceNodes: Seq[Chan[DepServiceNode[Transport]]] =
    for (address <- config.depServiceNodeAddresses)
      yield chan[DepServiceNode[Transport]](address, DepServiceNode.serializer)

  // Channels to the other leaders.
  private val otherLeaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses if a != address)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // The next available vertex id. When a leader receives a command, it assigns
  // it a vertex id using nextVertexId and then increments nextVertexId.
  @JSExport
  protected var nextVertexId: Int = 0

  // The state of each vertex that the leader knows about.
  val states = mutable.Map[VertexId, State[Transport]]()

  // The client table, which records the latest commands for each client.
  @JSExport
  protected val clientTable =
    new ClientTable[(Transport#Address, ClientPseudonym), Array[Byte]]()

  // If a leader commits a command in vertex A with a dependency on uncommitted
  // vertex B, then the leader sets a timer to recover vertex B. This prevents
  // a vertex from being forever stalled.
  @JSExport
  protected val recoverVertexTimers = mutable.Map[VertexId, Transport#Timer]()

  // The colocated Paxos proposer used to propose to the consensus service.
  @JSExport
  protected val proposer = new Proposer[Transport](
    address = config.proposerAddresses(index),
    transport = transport,
    logger = logger,
    config = config,
    options = options.proposerOptions,
    metrics = proposerMetrics
  )

  // Helpers ///////////////////////////////////////////////////////////////////
  private def willBeCommitted(vertexId: VertexId): Boolean = {
    states.get(vertexId) match {
      case None | Some(_: WaitingForDeps[_]) =>
        false
      case Some(_: WaitingForConsensus[_]) | Some(_: Committed[_]) => true
    }
  }

  private def onCommitted(
      vertexId: VertexId,
      value: scala.util.Try[Acceptor.VoteValue]
  ): Unit = {
    (value, states.get(vertexId)) match {
      case (scala.util.Failure(e), _) =>
        logger.fatal(
          s"Leader was unable to get a command chosen in vertex $vertexId. " +
            s"Error: $e."
        )

      case (_, None) =>
        logger.fatal(
          s"Leader got a value chosen in vertex $vertexId, but is not " +
            s"leading the vertex at all."
        )

      case (_, Some(_: WaitingForDeps[_])) =>
        logger.fatal(
          s"Leader got a value chosen in vertex $vertexId, but is " +
            s"waiting for dependencies."
        )

      case (_, Some(_: Committed[_])) =>
        logger.debug(
          s"Leader got a value chosen in vertex $vertexId, but a value was " +
            s"already chosen."
        )

      case (scala.util.Success(Acceptor.VoteValue(commandOrNoop, dependencies)),
            Some(_: WaitingForConsensus[_])) =>
        commit(vertexId, commandOrNoop, dependencies, informOthers = true)
    }
  }

  private def commit(
      vertexId: VertexId,
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId],
      informOthers: Boolean
  ): Unit = {
    metrics.committedCommandsTotal.inc()

    // Update the leader state.
    states(vertexId) = Committed(commandOrNoop, dependencies)

    // Notify the other replicas.
    if (informOthers) {
      for (leader <- otherLeaders) {
        leader.send(
          LeaderInbound().withCommit(
            Commit(vertexId = vertexId,
                   commandOrNoop = commandOrNoop,
                   dependency = dependencies.toSeq)
          )
        )
      }
    }

    // Stop any recovery timer for the current vertex, and start recovery
    // timers for any uncommitted vertices on which we depend.
    recoverVertexTimers.get(vertexId).foreach(_.stop())
    recoverVertexTimers -= vertexId
    for {
      v <- dependencies
      if !willBeCommitted(v)
      if !recoverVertexTimers.contains(v)
    } {
      recoverVertexTimers(v) = makeRecoverVertexTimer(v)
    }

    // Execute commands.
    val executable: Seq[VertexId] =
      dependencyGraph.commit(vertexId, (), dependencies)
    metrics.dependencyGraphNumVertices.set(dependencyGraph.numNodes)
    metrics.dependencyGraphNumEdges.set(dependencyGraph.numEdges)

    for (v <- executable) {
      import CommandOrNoop.Value
      states.get(v) match {
        case None | Some(_: WaitingForDeps[_]) |
            Some(_: WaitingForConsensus[_]) =>
          logger.fatal(
            s"Vertex $vertexId is ready for execution but the leader " +
              s"doesn't have a Committed entry for it."
          )

        case Some(committed: Committed[Transport]) => {
          committed.commandOrNoop.value match {
            case Value.Empty =>
              logger.fatal("Empty CommandOrNoop.")

            case Value.Noop(Noop()) =>
              // Noop.
              metrics.executedNoopsTotal.inc()

            case Value.Command(command: Command) =>
              val clientAddress = transport.addressSerializer.fromBytes(
                command.clientAddress.toByteArray
              )
              val clientIdentity = (clientAddress, command.clientPseudonym)
              clientTable.executed(clientIdentity, command.clientId) match {
                case ClientTable.Executed(_) =>
                  // Don't execute the same command twice.
                  metrics.repeatedCommandsTotal.inc()

                case ClientTable.NotExecuted =>
                  val output = stateMachine.run(command.command.toByteArray)
                  clientTable.execute(clientIdentity, command.clientId, output)
                  metrics.executedCommandsTotal.inc()

                  // The leader of the command instance returns the response to
                  // the client. If the leader is dead, then the client will
                  // eventually re-send its request and some other replica will
                  // reply, either from its client log or by getting the
                  // command chosen in a new instance.
                  if (index == v.leaderIndex) {
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
    }
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendDependencyRequestsTimer(
      dependencyRequest: DependencyRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendDependencyRequests [${dependencyRequest.vertexId}]",
      options.resendDependencyRequestsTimerPeriod,
      () => {
        metrics.resendDependencyRequestsTotalTotal.inc()
        for (depServiceNode <- depServiceNodes) {
          depServiceNode.send(
            DepServiceNodeInbound().withDependencyRequest(dependencyRequest)
          )
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeRecoverVertexTimer(vertexId: VertexId): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"recoverVertex [$vertexId]",
      Util.randomDuration(options.recoverVertexTimerMinPeriod,
                          options.recoverVertexTimerMaxPeriod),
      () => {
        // Sanity check and stop timers.
        states.get(vertexId) match {
          case None =>
          case Some(waitingForDeps: WaitingForDeps[_]) =>
            waitingForDeps.resendDependencyRequestsTimer.stop()

          case Some(_: WaitingForConsensus[_]) | Some(_: Committed[_]) =>
            logger.fatal(
              s"Leader recovering vertex $vertexId, but is either waiting " +
                s"for that vertex to be chosen, or that vertex has already " +
                s"been chosen."
            )
        }

        // Propose a noop to the consensus service.
        val noop = CommandOrNoop().withNoop(Noop())
        val deps = Set[VertexId]()
        val future = proposer.propose(vertexId, noop, deps)
        future.onComplete(onCommitted(vertexId, _))(transport.executionContext)

        // Update our state.
        states(vertexId) = WaitingForConsensus(
          commandOrNoop = noop,
          dependencies = deps,
          future = future
        )
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.ClientRequest(r)   => handleClientRequest(src, r)
      case Request.DependencyReply(r) => handleDependencyReply(src, r)
      case Request.Commit(r)          => handleCommit(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    metrics.requestsTotal.labels("ClientRequest").inc()

    // If we already have a response to this request in the client table, we
    // simply return it.
    val clientIdentity = (src, clientRequest.command.clientPseudonym)
    clientTable.executed(clientIdentity, clientRequest.command.clientId) match {
      case ClientTable.NotExecuted =>
      // Not executed yet, we'll have to get it chosen.

      case ClientTable.Executed(None) =>
      // Executed already but a stale command. We ignore this request.

      case ClientTable.Executed(Some(output)) =>
        // Executed already and is the most recent command. We relay the
        // response to the client.
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound()
            .withClientReply(
              ClientReply(
                clientPseudonym = clientRequest.command.clientPseudonym,
                clientId = clientRequest.command.clientId,
                result = ByteString.copyFrom(output)
              )
            )
        )
        return
    }

    // Create a new vertex id for this command.
    val vertexId = VertexId(index, nextVertexId)
    nextVertexId += 1

    // Send a request to the dependency service.
    val dependencyRequest =
      DependencyRequest(vertexId = vertexId, command = clientRequest.command)
    for (depServiceNode <- depServiceNodes) {
      depServiceNode.send(
        DepServiceNodeInbound().withDependencyRequest(dependencyRequest)
      )
    }

    // Update our state.
    states(vertexId) = WaitingForDeps(
      command = clientRequest.command,
      dependencyReplies = mutable.Map[DepServiceNodeIndex, DependencyReply](),
      resendDependencyRequestsTimer =
        makeResendDependencyRequestsTimer(dependencyRequest)
    )
  }

  private def handleDependencyReply(
      src: Transport#Address,
      dependencyReply: DependencyReply
  ): Unit = {
    metrics.requestsTotal.labels("DependencyReply").inc()

    states.get(dependencyReply.vertexId) match {
      case None | Some(_: WaitingForConsensus[_]) | Some(_: Committed[_]) =>
        logger.warn(
          s"Leader received DependencyReply for vertex " +
            s"${dependencyReply.vertexId}, but is not currently waiting for " +
            s"dependencies for that vertex."
        )

      case Some(waitingForDeps: WaitingForDeps[Transport]) =>
        // Wait for a quorum of responses.
        waitingForDeps.dependencyReplies(dependencyReply.depServiceNodeIndex) =
          dependencyReply
        if (waitingForDeps.dependencyReplies.size < config.quorumSize) {
          return
        }

        // Once we have a quorum of respones, we take the union of all returned
        // dependencies as the final set of dependencies.
        val dependencyReplies: Set[DependencyReply] =
          waitingForDeps.dependencyReplies.values.toSet
        val dependencies: Set[VertexId] =
          dependencyReplies.map(_.dependency.toSet).flatten
        metrics.dependencies.observe(dependencies.size)

        // Stop the running timers.
        waitingForDeps.resendDependencyRequestsTimer.stop()
        recoverVertexTimers.get(dependencyReply.vertexId).foreach(_.stop())
        recoverVertexTimers -= dependencyReply.vertexId

        // Propose our command and dependencies to the consensus service.
        val commandOrNoop = CommandOrNoop().withCommand(waitingForDeps.command)
        val future = proposer.propose(dependencyReply.vertexId,
                                      commandOrNoop,
                                      dependencies)
        future.onComplete(onCommitted(dependencyReply.vertexId, _))(
          transport.executionContext
        )

        // Update our state.
        states(dependencyReply.vertexId) = WaitingForConsensus(
          commandOrNoop = commandOrNoop,
          dependencies = dependencies,
          future = future
        )
    }
  }

  private def handleCommit(
      src: Transport#Address,
      c: Commit
  ): Unit = {
    metrics.requestsTotal.labels("Commit").inc()
    commit(c.vertexId,
           c.commandOrNoop,
           c.dependency.toSet,
           informOthers = false)
  }
}
