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
    proposerOptions: ProposerOptions
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendDependencyRequestsTimerPeriod = java.time.Duration.ofSeconds(1),
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

  val committedCommandsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_committed_commands_total")
    .help(
      "Total number of commands that were committed (with potential " +
        "duplicates)."
    )
    .register()

  val resendDependencyRequestsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_resend_dependency_requests_total")
    .help("Total number of times the leader resent DependencyRequest messages.")
    .register()

  val recoverVertexTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_recover_vertex_total")
    .help("Total number of times the leader recovered an instance.")
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

  // TODO(mwhittaker): Garbage collect these entries.
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

  // Channels to the replicas.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

  // The next available vertex id. When a leader receives a command, it assigns
  // it a vertex id using nextVertexId and then increments nextVertexId.
  @JSExport
  protected var nextVertexId: Int = 0

  // The state of each vertex that the leader knows about.
  val states = mutable.Map[VertexId, State[Transport]]()

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

      case (
          _,
          state @ (None | Some(_: WaitingForDeps[_]) | Some(_: Committed[_]))
          ) =>
        logger.fatal(
          s"Leader got a value chosen in vertex $vertexId, but is not " +
            s"currently waiting for consensus in this vertex. state is $state."
        )

      case (scala.util.Success(Acceptor.VoteValue(commandOrNoop, dependencies)),
            Some(_: WaitingForConsensus[_])) =>
        metrics.committedCommandsTotal.inc()

        // Send commit to replicas.
        for (replica <- replicas) {
          replica.send(
            ReplicaInbound().withCommit(
              Commit(vertexId = vertexId,
                     commandOrNoop = commandOrNoop,
                     dependency = dependencies.toSeq)
            )
          )
        }

        // Update our state.
        states -= vertexId
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
        metrics.resendDependencyRequestsTotal.inc()
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

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.ClientRequest(r)   => handleClientRequest(src, r)
      case Request.DependencyReply(r) => handleDependencyReply(src, r)
      case Request.Recover(r)         => handleRecover(src, r)
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

    // TODO(mwhittaker): Think harder about repeated client commands. The
    // leader isn't a replica, so it doesn't cache a response to an already
    // executed command. Moreover, we don't want leaders broadcasting which
    // commands are chosen to the other leaders. We could just have the command
    // chosen again. Assuming repeated commands are rare, this shouldn't be a
    // problem.

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
      case state @ (None | Some(_: WaitingForConsensus[_]) |
          Some(_: Committed[_])) =>
        logger.warn(
          s"Leader received DependencyReply for vertex " +
            s"${dependencyReply.vertexId}, but is not currently waiting for " +
            s"dependencies for that vertex. The state is $state."
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

        // Stop the running timers.
        waitingForDeps.resendDependencyRequestsTimer.stop()

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

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    metrics.requestsTotal.labels("Recover").inc()

    // Sanity check and stop timers.
    states.get(recover.vertexId) match {
      case None =>
      case Some(waitingForDeps: WaitingForDeps[_]) =>
        waitingForDeps.resendDependencyRequestsTimer.stop()

      case Some(_: WaitingForConsensus[_]) =>
        // If the leader is already waiting on consensus for this vertex, then
        // it has nothing to recover. A value will eventually be committed.
        return

      case Some(Committed(commandOrNoop, dependencies)) =>
        // We've already committed this instance. We can reply back immediately.
        val replica = chan[Replica[Transport]](src, Replica.serializer)
        replica.send(
          ReplicaInbound().withCommit(
            Commit(vertexId = recover.vertexId,
                   commandOrNoop = commandOrNoop,
                   dependency = dependencies.toSeq)
          )
        )
        return
    }

    // Propose a noop to the consensus service.
    val noop = CommandOrNoop().withNoop(Noop())
    val deps = Set[VertexId]()
    val future = proposer.propose(recover.vertexId, noop, deps)
    future.onComplete(onCommitted(recover.vertexId, _))(
      transport.executionContext
    )

    // Update our state.
    states(recover.vertexId) = WaitingForConsensus(
      commandOrNoop = noop,
      dependencies = deps,
      future = future
    )
  }
}
