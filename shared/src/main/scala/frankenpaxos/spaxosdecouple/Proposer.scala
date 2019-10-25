package frankenpaxos.spaxosdecouple

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.roundsystem.RoundSystem

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
                            batchSize: Int,
                            measureLatencies: Boolean
)

@JSExportAll
object ProposerOptions {
  val default = ProposerOptions(batchSize = 100,
    measureLatencies = true)
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
  protected val acks: mutable.Map[CommandBatch, mutable.Map[ExecutorId, Acknowledge]] = mutable.Map[CommandBatch, mutable.Map[ExecutorId, Acknowledge]]()

  // Stable ids
  @JSExport
  protected var stableBatches: mutable.Buffer[CommandBatch] = mutable.Buffer()

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  @JSExport
  protected var disseminatorQuorumSize: Int = config.f + 1

  protected var round: Int = 0

  @JSExport
  protected var growingBatch = mutable.Buffer[UniqueId]()

  @JSExport
  protected var pendingResendBatches = mutable.Buffer[UniqueId]()

// Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ProposerInbound.Request
    inbound.request match {
      case Request.ClientRequest(r)  => handleClientRequest(src, r)
      case Request.Acknowledge(r)    => handleAcknowledge(src, r)
      case Request.LeaderInfo(r)     => handleLeaderInfo(src, r)
      case Request.RequestBatch(r)   => handleRequestBatch(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  def handleRequestBatch(src: Transport#Address, requestBatch: RequestBatch): Unit = {
    for (executor <- executors) {
      executor.send(ExecutorInbound().withForward(Forward(requestBatch)))
    }
  }

  def handleClientRequest(src: Transport#Address, clientRequest: ClientRequest) = {
    for (executor <- executors) {
      val requestBatch = RequestBatch(Seq(clientRequest))
      executor.send(ExecutorInbound().withForward(Forward(requestBatch)))
    }
  }

  def handleAcknowledge(src: Transport#Address, acknowledge: Acknowledge) = {
    val executorMap: mutable.Map[ExecutorId, Acknowledge] = acks.getOrElse(acknowledge.commandBatch, mutable.Map())
    executorMap.put(config.executorAddresses.indexOf(src), acknowledge)

    acks.put(acknowledge.commandBatch, executorMap)

    if (acks.getOrElse(acknowledge.commandBatch, mutable.Map()).size >= disseminatorQuorumSize && !stableBatches.contains(acknowledge.commandBatch)) {
      stableBatches.append(acknowledge.commandBatch)

      val leader = leaders(roundSystem.leader(round))
      leader.send(
        LeaderInbound().withClientRequestBatch(
          ClientRequestBatch(acknowledge.commandBatch))
      )
    }
  }

  def handleLeaderInfo(src: Transport#Address, leaderInfo: LeaderInfo): Unit = {
    round = leaderInfo.round
  }
}
