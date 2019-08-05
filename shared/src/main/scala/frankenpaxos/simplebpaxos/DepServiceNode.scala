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
import frankenpaxos.util
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
    // A dependency service node garbage collects its conflict index every
    // `garbageCollectEveryNCommands` commands that it receives.
    garbageCollectEveryNCommands: Int
)

@JSExportAll
object DepServiceNodeOptions {
  val default = DepServiceNodeOptions(
    garbageCollectEveryNCommands = 10000
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

  // This compacted conflict index stores all of the commands seen so far. When
  // a dependency service node receives a new command, it uses the conflict
  // index to efficiently compute dependencies.
  @JSExport
  protected val conflictIndex = new CompactConflictIndex(
    stateMachine.conflictIndex[VertexId]()
  )(VertexIdPrefixSet.factory(config.leaderAddresses.size))

  // The number of commands that the dependency service node has received since
  // the last time it garbage collected.  Every
  // `options.garbageCollectEveryNCommands` commands, this value is reset and
  // the conflict index is garbage collected.
  @JSExport
  protected var numCommandsPendingGc: Int = 0

  // The watermark used to garbage collect the conflict index.
  @JSExport
  protected var gcWatermark: VertexIdPrefixSet = VertexIdPrefixSet(
    config.leaderAddresses.size
  )

  // dependencies caches the dependencies computed by the conflict index. If a
  // dependency service node receives a command more than once, it returns the
  // same set of dependencies.
  @JSExport
  protected val dependenciesCache =
    mutable.Map[VertexId, VertexIdPrefixSetProto]()

  // The cache garbage collection watermark. If n is the number of leaders,
  // then cacheGcQuorumWatermarkVector.watermark() is a vector of length n. Say
  // the ith entry of the watermark is j. Then all vertices with leader index i
  // and id less than j can be garbage collected from the cache. Note that
  // cache garbage collection happens independently of conflict index garbage
  // collection.
  @JSExport
  protected val cacheGcQuorumWatermarkVector = new util.QuorumWatermarkVector(
    n = config.replicaAddresses.size,
    depth = config.leaderAddresses.size
  )

  // cacheGcWatermark is the cached value of
  // cacheGcQuorumWatermarkVector.watermark(). We cache it because we refer to
  // it often, but it's ok if its stale.
  @JSExport
  protected var cacheGcWatermark: Seq[Int] =
    cacheGcQuorumWatermarkVector.watermark(quorumSize = config.f + 1)

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
      case Request.GarbageCollect(r) =>
        handleGarbageCollect(src, r)
        "GarbageCollect"
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
    val vertexId = dependencyRequest.vertexId

    // Ignore garbage collected vertices.
    if (vertexId.id < cacheGcWatermark(vertexId.leaderIndex)) {
      logger.debug(
        s"DepServiceNode received a DependencyRequest message for vertex " +
          s"$vertexId but has a cache watermark of $cacheGcWatermark, so the " +
          s"vertex has already been garbage collected. The message is being " +
          s"ignored."
      )
      return
    }

    val dependencies = dependenciesCache.get(vertexId) match {
      case Some(dependencies) => dependencies

      case None =>
        val command = dependencyRequest.command.command.toByteArray
        val dependencies = conflictIndex.getConflicts(command)
        val proto = dependencies.toProto()
        conflictIndex.put(vertexId, command)
        dependenciesCache(vertexId) = proto
        metrics.dependencies.observe(dependencies.size)
        metrics.uncompactedDependencies.observe(dependencies.uncompactedSize)
        proto
    }

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    leader.send(
      LeaderInbound().withDependencyReply(
        DependencyReply(vertexId = dependencyRequest.vertexId,
                        depServiceNodeIndex = index,
                        dependencies = dependencies)
      )
    )

    numCommandsPendingGc += 1
    if (numCommandsPendingGc % options.garbageCollectEveryNCommands == 0) {
      conflictIndex.garbageCollect(gcWatermark)
      gcWatermark = conflictIndex.watermark()
      numCommandsPendingGc = 0
    }
  }

  private def handleGarbageCollect(
      src: Transport#Address,
      garbageCollect: GarbageCollect
  ): Unit = {
    // Update the GC watermark.
    cacheGcQuorumWatermarkVector.update(
      garbageCollect.replicaIndex,
      garbageCollect.frontier
    )
    cacheGcWatermark =
      cacheGcQuorumWatermarkVector.watermark(quorumSize = config.f + 1)

    // Garbage collect all cache entries lower than the watermark.
    dependenciesCache.retain({
      case (vertexId, _) =>
        vertexId.id >= cacheGcWatermark(vertexId.leaderIndex)
    })
  }
}
