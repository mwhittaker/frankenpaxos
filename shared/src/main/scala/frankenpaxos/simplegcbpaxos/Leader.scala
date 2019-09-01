package frankenpaxos.simplegcbpaxos

import VertexIdHelpers.vertexIdOrdering
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.thrifty.ThriftySystem
import scala.collection.mutable
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
    thriftySystem: ThriftySystem,
    resendDependencyRequestsTimerPeriod: java.time.Duration
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    thriftySystem = ThriftySystem.NotThrifty,
    resendDependencyRequestsTimerPeriod = java.time.Duration.ofSeconds(1)
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val proposalsSentTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_leader_proposals_sent_total")
    .help("Total number of proposals sent to a proposer.")
    .register()

  val resendDependencyRequestsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_leader_resend_dependency_requests_total")
    .help("Total number of times the leader resent DependencyRequest messages.")
    .register()
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer

  type ClientPseudonym = Int
  type DepServiceNodeIndex = Int

  @JSExportAll
  case class State[Transport <: frankenpaxos.Transport[Transport]](
      commandOrSnapshot: CommandOrSnapshot,
      dependencyReplies: mutable.Map[DepServiceNodeIndex, DependencyReply],
      resendDependencyRequestsTimer: Transport#Timer
  )
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
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

  // Channels to the dependency service nodes, indexed by address.
  private val depServiceNodesByAddress
    : Map[Transport#Address, Chan[DepServiceNode[Transport]]] = {
    for (a <- config.depServiceNodeAddresses)
      yield a -> chan[DepServiceNode[Transport]](a, DepServiceNode.serializer)
  }.toMap

  // Channel to accompanying proposer.
  private val proposer: Chan[Proposer[Transport]] = chan[Proposer[Transport]](
    config.proposerAddresses(index),
    Proposer.serializer
  )

  // The next available vertex id. When a leader receives a command, it assigns
  // it a vertex id using nextVertexId and then increments nextVertexId. Note
  // that vertex ids are assigned contiguously. This is important for garbage
  // collection.
  @JSExport
  protected var nextVertexId: Int = 0

  // The state of each vertex that the leader knows about.
  val states = mutable.Map[VertexId, State[Transport]]()

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

  // Helpers ///////////////////////////////////////////////////////////////////
  private def getAndIncrementNextVertexId(): VertexId = {
    val vertexId = nextVertexId
    nextVertexId += 1
    VertexId(leaderIndex = index, id = vertexId)
  }

  private def thriftyDepServiceNodes(
      n: Int
  ): Set[Chan[DepServiceNode[Transport]]] = {
    // TODO(mwhittaker): Add heartbeats to real delays.
    val delays: Map[Transport#Address, java.time.Duration] = {
      for (a <- config.depServiceNodeAddresses)
        yield a -> java.time.Duration.ofSeconds(0)
    }.toMap
    options.thriftySystem.choose(delays, n).map(depServiceNodesByAddress(_))
  }

  private def handleRequest(
      src: Transport#Address,
      commandOrSnapshot: CommandOrSnapshot
  ): Unit = {
    // TODO(mwhittaker): Think harder about repeated client commands. The
    // leader isn't a replica, so it doesn't cache a response to an already
    // executed command. Moreover, we don't want leaders broadcasting which
    // commands are chosen to the other leaders. We could just have the command
    // chosen again. Assuming repeated commands are rare, this shouldn't be a
    // problem.

    // Create a new vertex id for this command.
    val vertexId = getAndIncrementNextVertexId()

    // Send a request to the dependency service.
    val dependencyRequest =
      DependencyRequest(vertexId = vertexId,
                        commandOrSnapshot = commandOrSnapshot)
    thriftyDepServiceNodes(config.quorumSize).foreach(
      _.send(DepServiceNodeInbound().withDependencyRequest(dependencyRequest))
    )

    // Update our state.
    states(vertexId) = State(
      commandOrSnapshot = commandOrSnapshot,
      dependencyReplies = mutable.Map[DepServiceNodeIndex, DependencyReply](),
      resendDependencyRequestsTimer =
        makeResendDependencyRequestsTimer(dependencyRequest)
    )
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request

    val startNanos = System.nanoTime
    val label = inbound.request match {
      case Request.ClientRequest(r) =>
        handleClientRequest(src, r)
        "ClientRequest"
      case Request.SnapshotRequest(r) =>
        handleSnapshotRequest(src, r)
        "SnapshotRequest"
      case Request.DependencyReply(r) =>
        handleDependencyReply(src, r)
        "DependencyReply"
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
    val stopNanos = System.nanoTime
    metrics.requestsTotal.labels(label).inc()
    metrics.requestsLatency
      .labels(label)
      .observe((stopNanos - startNanos).toDouble / 1000000)
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    handleRequest(src, CommandOrSnapshot().withCommand(clientRequest.command))
  }

  private def handleSnapshotRequest(
      src: Transport#Address,
      snapshotRequest: SnapshotRequest
  ): Unit = {
    handleRequest(src, CommandOrSnapshot().withSnapshot(Snapshot()))
  }

  private def handleDependencyReply(
      src: Transport#Address,
      dependencyReply: DependencyReply
  ): Unit = {
    states.get(dependencyReply.vertexId) match {
      case None =>
        logger.debug(
          s"Leader received DependencyReply for vertex " +
            s"${dependencyReply.vertexId}, but is not currently waiting for " +
            s"dependencies for that vertex. This means that either (a) the " +
            s"leader has already computed dependencies for the vertex and " +
            s"subsequently garbage collected information about it or (b) the " +
            s"leader never received any information about this vertex " +
            s"before. In either case, we ignore the dependency reply."
        )

      case Some(state: State[Transport]) =>
        // Wait for a quorum of responses.
        state.dependencyReplies(dependencyReply.depServiceNodeIndex) =
          dependencyReply
        if (state.dependencyReplies.size < config.quorumSize) {
          return
        }

        // Once we have a quorum of respones, we take the union of all returned
        // dependencies as the final set of dependencies.
        val dependencyReplies: Set[DependencyReply] =
          state.dependencyReplies.values.toSet
        val dependencies: VertexIdPrefixSet = dependencyReplies
          .map(reply => VertexIdPrefixSet.fromProto(reply.dependencies))
          .fold(VertexIdPrefixSet(config.leaderAddresses.size))({
            case (x, y) => x.union(y)
          })

        // Stop the running timers.
        state.resendDependencyRequestsTimer.stop()

        // Propose our command and dependencies to the consensus service.
        proposer.send(
          ProposerInbound().withPropose(
            Propose(vertexId = dependencyReply.vertexId,
                    commandOrSnapshot = state.commandOrSnapshot,
                    dependencies = dependencies.toProto)
          )
        )
        metrics.proposalsSentTotal.inc()

        // Forget about the command.
        states -= dependencyReply.vertexId
    }
  }
}
