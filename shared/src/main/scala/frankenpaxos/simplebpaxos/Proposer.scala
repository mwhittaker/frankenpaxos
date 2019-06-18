package frankenpaxos.simplebpaxos

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.PrometheusCollectors
import scala.scalajs.js.annotation._

@JSExportAll
object ProposerInboundSerializer extends ProtoSerializer[ProposerInbound] {
  type A = ProposerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class ProposerOptions(
    // TODO(mwhittaker): Add options.
)

@JSExportAll
object ProposerOptions {
  // TODO(mwhittaker): Add options.
  val default = ProposerOptions()
}

@JSExportAll
class ProposerMetrics(collectors: Collectors) {
  // TODO(mwhittaker): Add metrics.
}

@JSExportAll
object Proposer {
  val serializer = ProposerInboundSerializer
}

@JSExportAll
class Proposer[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    // TODO(mwhittaker): Add config.
    // config: Config[Transport],
    options: ProposerOptions = ProposerOptions.default,
    metrics: ProposerMetrics = new ProposerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Proposer._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ProposerInbound
  override def serializer = Proposer.serializer

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ProposerInbound
  ): Unit = {
    import ProposerInbound.Request
    inbound.request match {
      case Request.Phase1B(r) => handlePhase1b(src, r)
      case Request.Phase2B(r) => handlePhase2b(src, r)
      case Request.Empty => {
        logger.fatal("Empty ProposerInbound encountered.")
      }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = ???

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = ???
}
