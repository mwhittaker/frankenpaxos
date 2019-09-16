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
import frankenpaxos.util.TopK
import frankenpaxos.util.TopOne
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
    // If topKDependencies is -1, then a dependency service node returns every
    // dependency of a command. If topKDependencies is equal to k for some k >
    // 0, then a dependency service node only returns the top-k dependencies
    // for every leader.
    topKDependencies: Int,
    // If `unsafeReturnNoDependencies` is true, dependency service nodes return
    // no dependencies for every command. As the name suggests, this is unsafe
    // and breaks the protocol. It should be used only for performance
    // debugging and evaluation.
    unsafeReturnNoDependencies: Boolean,
    measureLatencies: Boolean
)

@JSExportAll
object DepServiceNodeOptions {
  val default = DepServiceNodeOptions(
    topKDependencies = 1,
    unsafeReturnNoDependencies = false,
    measureLatencies = true
  )
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
        "computes for a command. This is the number of dependencies that " +
        "cannot be represented compactly."
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
  protected val conflictIndex = stateMachine.topKConflictIndex(
    options.topKDependencies,
    config.leaderAddresses.size,
    VertexIdHelpers.like
  )

  // dependencies caches the dependencies computed by the conflict index. If a
  // dependency service node receives a command more than once, it returns the
  // same set of dependencies.
  @JSExport
  protected val dependenciesCache =
    mutable.Map[VertexId, VertexIdPrefixSetProto]()

  // Handlers //////////////////////////////////////////////////////////////////
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

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: DepServiceNodeInbound
  ): Unit = {
    import DepServiceNodeInbound.Request

    val label = inbound.request match {
      case Request.DependencyRequest(r) => "DependencyRequest"
      case Request.Empty => {
        logger.fatal("Empty DepServiceNodeInbound encountered.")
      }
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.DependencyRequest(r) =>
          handleDependencyRequest(src, r)
        case Request.Empty => {
          logger.fatal("Empty DepServiceNodeInbound encountered.")
        }
      }
    }
  }

  private def handleDependencyRequest(
      src: Transport#Address,
      dependencyRequest: DependencyRequest
  ): Unit = {
    // If options.unsafeReturnNoDependencies is true, we return no dependencies
    // and return immediately. This makes the protocol unsafe, but is useful
    // for performance debugging.
    val leader = chan[Leader[Transport]](src, Leader.serializer)
    if (options.unsafeReturnNoDependencies) {
      leader.send(
        LeaderInbound().withDependencyReply(
          DependencyReply(
            vertexId = dependencyRequest.vertexId,
            depServiceNodeIndex = index,
            dependencies =
              VertexIdPrefixSet(config.leaderAddresses.size).toProto()
          )
        )
      )
      return
    }

    val vertexId = dependencyRequest.vertexId
    val dependencies = dependenciesCache.get(vertexId) match {
      case Some(dependencies) =>
        dependencies

      case None =>
        val bytes = dependencyRequest.command.command.toByteArray
        val dependencies = if (options.topKDependencies == 1) {
          VertexIdPrefixSet.fromTopOne(
            conflictIndex.getTopOneConflicts(bytes)
          )
        } else {
          VertexIdPrefixSet.fromTopK(conflictIndex.getTopKConflicts(bytes))
        }
        dependencies.subtractOne(dependencyRequest.vertexId)
        val proto = dependencies.toProto()
        conflictIndex.put(vertexId, bytes)
        dependenciesCache(vertexId) = proto
        metrics.dependencies.observe(dependencies.size)
        metrics.uncompactedDependencies.observe(dependencies.uncompactedSize)
        proto
    }

    leader.send(
      LeaderInbound().withDependencyReply(
        DependencyReply(vertexId = dependencyRequest.vertexId,
                        depServiceNodeIndex = index,
                        dependencies = dependencies)
      )
    )
  }
}
