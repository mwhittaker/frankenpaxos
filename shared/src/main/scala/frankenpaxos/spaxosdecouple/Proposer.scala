package frankenpaxos.spaxosdecouple

import collection.mutable
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
object Proposer {
  val serializer = ProposerInboundSerializer
}

@JSExportAll
class ProposerMetrics(collectors: Collectors) {
}

@JSExportAll
case class ProposerOptions(
)

@JSExportAll
object ProposerOptions {
  val default = ProposerOptions()
}

@JSExportAll
class Proposer[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ProposerOptions = ProposerOptions.default,
    metrics: ProposerMetrics = new ProposerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  override type InboundMessage = ProposerInbound
  override val serializer = Proposer.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the acceptor's id.
  logger.check(config.proposerAddresses.contains(address))
  private val proposerId = config.proposerAddresses.indexOf(address)

  // Channels to the leaders.
  private val leaders: Map[Int, Chan[Leader[Transport]]] = {
    for ((leaderAddress, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](leaderAddress, Leader.serializer)
  }.toMap

  // Executors
  private val executors: Seq[Chan[Executor[Transport]]] = {
    for (executorAddress <- config.executorAddresses)
      yield chan[Executor[Transport]](executorAddress, Executor.serializer)
  }

  type ExecutorId = Int

  // The acknowledge messages received from Execution Service
  @JSExport
  protected val acks: mutable.Map[UniqueId, mutable.Map[ExecutorId, Acknowledge]] = mutable.Map[UniqueId, mutable.Map[ExecutorId, Acknowledge]]()

  // Stable ids
  @JSExport
  protected var stableIds: mutable.Buffer[UniqueId] = mutable.Buffer()

  // Quorum size for disseminators
  @JSExport
  protected var disseminatorQuorumSize: Int = config.f + 1

  protected var round: Int = 0

// Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ProposerInbound.Request
    inbound.request match {
      case Request.ClientRequest(r)  => handleClientRequest(src, r)
      case Request.Acknowledge(r)    => handleAcknowledge(src, r)
      case Request.LeaderInfo(r)     => handleLeaderInfo(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  def handleClientRequest(src: Transport#Address, clientRequest: ClientRequest) = {
    for (executor <- executors) {
      executor.send(ExecutorInbound().withForward(Forward(clientRequest = clientRequest)))
    }

    for ((_, leader) <- leaders) {
      leader.send(LeaderInbound().withProposal(Proposal(clientRequest.uniqueId, round)))
    }
  }

  def handleAcknowledge(src: Transport#Address, acknowledge: Acknowledge) = {
    val executorMap: mutable.Map[ExecutorId, Acknowledge] = acks.getOrElse(acknowledge.uniqueId, mutable.Map())
    executorMap.put(config.executorAddresses.indexOf(src), acknowledge)

    acks.put(acknowledge.uniqueId, executorMap)

    if (acks.getOrElse(acknowledge.uniqueId, mutable.Map()).size >= disseminatorQuorumSize && !stableIds.contains(acknowledge.uniqueId)) {
      stableIds.append(acknowledge.uniqueId)

      // Request is simultaneously sent to the leader when receiving a client request
      /*for ((_, leader) <- leaders) {
        leader.send(LeaderInbound().withProposal(Proposal(acknowledge.uniqueId, round)))
      }*/
    }
  }

  def handleLeaderInfo(src: Transport#Address, leaderInfo: LeaderInfo): Unit = {
    round = leaderInfo.round
  }
}
