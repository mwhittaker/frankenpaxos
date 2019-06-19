package frankenpaxos.simplebpaxos

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.statemachine.StateMachine
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
    // TODO(mwhittaker): Add options.
)

@JSExportAll
object LeaderOptions {
  // TODO(mwhittaker): Add options.
  val default = LeaderOptions()
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  // TODO(mwhittaker): Add metrics.
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    stateMachine: StateMachine,
    // TODO(mwhittaker): Add dependency graph.
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Leader._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

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
  ): Unit = ???

  private def handleDependencyReply(
      src: Transport#Address,
      dependencyReply: DependencyReply
  ): Unit = ???

  private def handleCommit(
      src: Transport#Address,
      commit: Commit
  ): Unit = ???
}
