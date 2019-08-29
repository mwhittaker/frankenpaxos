
package frankenpaxos.spaxosdecouple

import com.google.protobuf.ByteString

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.PrometheusCollectors

import scala.scalajs.js.annotation._

@JSExportAll
object DisseminatorInboundSerializer extends ProtoSerializer[DisseminatorInbound] {
  type A = DisseminatorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Disseminator {
  val serializer = DisseminatorInboundSerializer
}

@JSExportAll
class DisseminatorMetrics(collectors: Collectors) {
}

@JSExportAll
case class DisseminatorOptions()

@JSExportAll
object DisseminatorOptions {
  val default = DisseminatorOptions()
}

@JSExportAll
class Disseminator[Transport <: frankenpaxos.Transport[Transport]](
                                                                    address: Transport#Address,
                                                                    transport: Transport,
                                                                    logger: Logger,
                                                                    config: Config[Transport],
                                                                    options: DisseminatorOptions = DisseminatorOptions.default,
                                                                    metrics: DisseminatorMetrics = new DisseminatorMetrics(PrometheusCollectors)
                                                                  ) extends Actor(address, transport, logger) {
  override type InboundMessage = DisseminatorInbound
  override val serializer = Disseminator.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the acceptor's id.
  logger.check(config.disseminatorAddresses.contains(address))
  private val disseminatorId = config.disseminatorAddresses.indexOf(address)

  // Channels to the leaders.
  private val leaders: Map[Int, Chan[Leader[Transport]]] = {
    for ((leaderAddress, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](leaderAddress, Leader.serializer)
  }.toMap

  private val proposers: Seq[Chan[Proposer[Transport]]] = {
    for (proposerAddress <- config.proposerAddresses)
      yield chan[Proposer[Transport]](proposerAddress, Proposer.serializer)
  }

  // Requests received from proposers to be disseminated
  @JSExport
  protected var requestsDisseminated: mutable.Set[ClientRequest] = mutable.Set()

  @JSExport
  protected var idRequestMap: mutable.Map[UniqueId, ByteString] = mutable.Map()

// Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import DisseminatorInbound.Request
    inbound.request match {
      case Request.Forward(r)        => handleForwardRequest(src, r)
      case Request.GetRequest(r)     => handleGetRequest(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  def handleForwardRequest(src: Transport#Address, forward: Forward) = {
    requestsDisseminated.add(forward.clientRequest)
    idRequestMap.put(forward.clientRequest.uniqueId, forward.clientRequest.command)

    // Broadcast acknowledgement to all proposers, tunable setting to only send to replica that sent the Forward message
    for (proposer <- proposers) {
      proposer.send(ProposerInbound().withAcknowledge(Acknowledge(uniqueId = forward.clientRequest.uniqueId)))
    }
  }

  def handleGetRequest(src: Transport#Address, getRequest: GetRequest): Unit = {
    if (idRequestMap.contains(getRequest.uniqueId)) {
      val executor = chan[Executor[Transport]](src, Executor.serializer)
      executor.send(ExecutorInbound().withSendRequest(SendRequest(getRequest.uniqueId, idRequestMap.get(getRequest.uniqueId).get)))
    }
  }
}

