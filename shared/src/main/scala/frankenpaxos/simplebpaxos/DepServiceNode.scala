package frankenpaxos.simplebpaxos

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
    .name("simple_bpaxos_dep_service_node_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_dep_service_node_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_dep_service_node_dependencies")
    .help(
      "The number of dependencies that a dependency service node computes " +
        "for a command."
    )
    .register()

  val uncompactedDependencies: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_dep_service_node_uncompacted_dependencies")
    .help(
      "The number of uncompacted dependencies that a dependency service node " +
        "computes for a command."
    )
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

  // This conflict index stores all of the commands seen so far. When a
  // dependency service node receives a new command, it uses the conflict index
  // to efficiently compute dependencies.
  @JSExport
  protected val conflictIndex = stateMachine.conflictIndex[VertexId]()

  // dependencies caches the dependencies computed by the conflict index. If a
  // dependency service node receives a command more than once, it returns the
  // same set of dependencies.
  @JSExport
  protected val dependenciesCache = mutable.Map[VertexId, VertexIdPrefixSet]()

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: DepServiceNodeInbound
  ): Unit = {
    import DepServiceNodeInbound.Request
    val startNanos = System.nanoTime
    val label = inbound.request match {
      case Request.DependencyRequest(r) =>
        handleDependencyRequest(src, r)
        "DependencyRequest"
      case Request.Empty => {
        logger.fatal("Empty DepServiceNodeInbound encountered.")
      }
    }
    val stopNanos = System.nanoTime
    metrics.requestsTotal.labels(label).inc()
    metrics.requestsLatency
      .labels(label)
      .observe((stopNanos - startNanos).toDouble / 1000000)
  }

  private def handleDependencyRequest(
      src: Transport#Address,
      dependencyRequest: DependencyRequest
  ): Unit = {
    // TODO(mwhittaker): Compute dependencies as a VertexIdPrefixSet directly,
    // while garbage collecting old entries. This will likely involve a rewrite
    // of the conflict index trait.
    val vertexId = dependencyRequest.vertexId
    val dependencies = dependenciesCache.get(vertexId) match {
      case Some(dependencies) => dependencies

      case None =>
        val command = dependencyRequest.command.command.toByteArray
        val dependencies = VertexIdPrefixSet(
          config.leaderAddresses.size,
          conflictIndex.getConflicts(command)
        )
        conflictIndex.put(vertexId, command)
        dependenciesCache(vertexId) = dependencies
        metrics.dependencies.observe(dependencies.size)
        metrics.uncompactedDependencies.observe(dependencies.size)
        dependencies
    }

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    leader.send(
      LeaderInbound().withDependencyReply(
        DependencyReply(vertexId = dependencyRequest.vertexId,
                        depServiceNodeIndex = index,
                        dependencies = dependencies.toProto)
      )
    )
  }
}
