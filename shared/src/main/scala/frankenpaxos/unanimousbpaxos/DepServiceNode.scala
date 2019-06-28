package frankenpaxos.unanimousbpaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.statemachine.StateMachine
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object DepServiceNodeInboundSerializer
    extends ProtoSerializer[DepServiceNodeInbound] {
  type A = DepServiceNodeInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class DepServiceNodeOptions(
    // TODO(mwhittaker): Add options.
)

@JSExportAll
object DepServiceNodeOptions {
  // TODO(mwhittaker): Add options.
  val default = DepServiceNodeOptions()
}

@JSExportAll
class DepServiceNodeMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_dep_service_node_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val duplicateRequestTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_dep_service_node_duplicate_request_total")
    .help(
      "Total number of requests for a command that the dependency service " +
        "node has already processed."
    )
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("unanimous_bpaxos_dep_service_node_dependencies")
    .help("The number of dependencies that a command has.")
    .register()
}

@JSExportAll
object DepServiceNode {
  val serializer = DepServiceNodeInboundSerializer
}

@JSExportAll
class DepServiceNode[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    stateMachine: StateMachine,
    options: DepServiceNodeOptions = DepServiceNodeOptions.default,
    metrics: DepServiceNodeMetrics = new DepServiceNodeMetrics(
      PrometheusCollectors
    )
) extends Actor(address, transport, logger) {
  import DepServiceNode._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = DepServiceNodeInbound
  override def serializer = DepServiceNode.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and get our index.
  logger.check(config.valid())
  logger.check(config.depServiceNodeAddresses.contains(address))
  private val index = config.depServiceNodeAddresses.indexOf(address)

  // The co-located acceptor.
  private val acceptor: Chan[Acceptor[Transport]] = chan[Acceptor[Transport]](
    config.acceptorAddresses(index),
    Acceptor.serializer
  )

  // This conflict index stores all of the commands seen so far. When a
  // dependency service node receives a new command, it uses the conflict index
  // to efficiently compute dependencies.
  @JSExport
  protected val conflictIndex = stateMachine.conflictIndex[VertexId]()

  // dependencies caches the dependencies computed by the conflict index. If a
  // dependency service node receives a command more than once, it returns the
  // same set of dependencies.
  @JSExport
  protected val dependenciesCache = mutable.Map[VertexId, Set[VertexId]]()

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: DepServiceNodeInbound
  ): Unit = {
    import DepServiceNodeInbound.Request
    inbound.request match {
      case Request.DependencyRequest(r) => handleDependencyRequest(src, r)
      case Request.Empty => {
        logger.fatal("Empty DepServiceNodeInbound encountered.")
      }
    }
  }

  private def handleDependencyRequest(
      src: Transport#Address,
      dependencyRequest: DependencyRequest
  ): Unit = {
    metrics.requestsTotal.labels("DependencyRequest").inc()

    val vertexId = dependencyRequest.vertexId
    val dependencies = dependenciesCache.get(vertexId) match {
      case Some(dependencies) =>
        metrics.duplicateRequestTotal.inc()
        dependencies

      case None =>
        val command = dependencyRequest.command.command.toByteArray
        val dependencies = conflictIndex.getConflicts(vertexId, command)
        conflictIndex.put(vertexId, command)
        dependenciesCache(vertexId) = dependencies
        metrics.dependencies.observe(dependencies.size)
        dependencies
    }

    acceptor.send(
      AcceptorInbound().withFastProposal(
        FastProposal(
          vertexId = dependencyRequest.vertexId,
          value = VoteValueProto(commandOrNoop = CommandOrNoop()
                                   .withCommand(dependencyRequest.command),
                                 dependency = dependencies.toSeq)
        )
      )
    )
  }
}
