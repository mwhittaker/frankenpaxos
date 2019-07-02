package frankenpaxos.simplebpaxos

import VertexIdHelpers.vertexIdOrdering
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
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
    resendDependencyRequestsTimerPeriod: java.time.Duration
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendDependencyRequestsTimerPeriod = java.time.Duration.ofSeconds(1)
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

  val proposalsSentTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_proposals_sent_total")
    .help("Total number of proposals sent to a proposer.")
    .register()

  val resendDependencyRequestsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_leader_resend_dependency_requests_total")
    .help("Total number of times the leader resent DependencyRequest messages.")
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

  // TODO(mwhittaker): Garbage collect these entries.
  @JSExportAll
  case class Proposed[Transport <: frankenpaxos.Transport[Transport]]()
      extends State[Transport]
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

  // Channel to accompanying proposer.
  private val proposer: Chan[Proposer[Transport]] =
    chan[Proposer[Transport]](config.proposerAddresses(index),
                              Proposer.serializer)

  // The next available vertex id. When a leader receives a command, it assigns
  // it a vertex id using nextVertexId and then increments nextVertexId.
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

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.ClientRequest(r) =>
        metrics.requestsTotal.labels("ClientRequest").inc()
        handleClientRequest(src, r)
      case Request.DependencyReply(r) =>
        metrics.requestsTotal.labels("DependencyReply").inc()
        handleDependencyReply(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
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
    states.get(dependencyReply.vertexId) match {
      case state @ (None | Some(_: Proposed[_])) =>
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
        proposer.send(
          ProposerInbound().withPropose(
            Propose(vertexId = dependencyReply.vertexId,
                    command = waitingForDeps.command,
                    dependency = dependencies.toSeq)
          )
        )

        // Update our state.
        states(dependencyReply.vertexId) = Proposed()
    }
  }
}
