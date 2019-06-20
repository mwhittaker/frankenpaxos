package frankenpaxos.simplebpaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.PrometheusCollectors
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
  // TODO(mwhittaker): Add metrics.
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
    val vertexId = dependencyRequest.vertexId
    val dependencies = dependenciesCache.get(vertexId) match {
      case Some(dependencies) => dependencies

      case None =>
        val command = dependencyRequest.command.command.toByteArray
        conflictIndex.put(vertexId, command)
        val dependencies = conflictIndex.getConflicts(vertexId, command)
        dependenciesCache(vertexId) = dependencies
        dependencies
    }

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    leader.send(
      LeaderInbound().withDependencyReply(
        DependencyReply(vertexId = dependencyRequest.vertexId,
                        depServiceNodeIndex = index,
                        dependency = dependencies.toSeq)
      )
    )
  }
}
