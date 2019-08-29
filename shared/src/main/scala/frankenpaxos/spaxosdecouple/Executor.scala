package frankenpaxos.spaxosdecouple

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._

@JSExportAll
object ExecutorInboundSerializer extends ProtoSerializer[ExecutorInbound] {
  type A = ExecutorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Executor {
  val serializer = ExecutorInboundSerializer
}

@JSExportAll
class ExecutorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val batchesTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_acceptor_batches_total")
    .help("Total number of ProposeRequest batches processed.")
    .register()

  val proposeRequestsInBatchesTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_acceptor_propose_requests_in_batches_total")
    .help("Total number of ProposeRequests processed in a batch.")
    .register()
}

@JSExportAll
case class ExecutorOptions(
    // With Fast MultiPaxos, it's possible that two clients concurrently
    // propose two conflicting commands and that those commands arrive at
    // acceptors in different orders preventing either from being chosen. This
    // is called a conflict, and the performance of Fast MultiPaxos degrades
    // as the number of conflicts increases.
    //
    // As a heuristic to avoid conflicts, we have acceptors buffer messages and
    // process them in batches in a deterministic order. Every `waitPeriod`
    // seconds, an acceptor forms a batch of all propose requests that are
    // older than `waitStagger`, sorts them deterministically, and process
    // them.
    //
    // TODO(mwhittaker): I don't think waitStagger is actually useful. Verify
    // that it's pointless and remove it.
    // TODO(mwhittaker): Is there a smarter way to reduce the number of
    // conflicts?
    waitPeriod: java.time.Duration,
    waitStagger: java.time.Duration
)

@JSExportAll
object ExecutorOptions {
  val default = ExecutorOptions(
    waitPeriod = java.time.Duration.ofMillis(25),
    waitStagger = java.time.Duration.ofMillis(25)
  )
}

@JSExportAll
class Executor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ExecutorOptions = ExecutorOptions.default,
    metrics: ExecutorMetrics = new ExecutorMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  override type InboundMessage = ExecutorInbound
  override val serializer = Executor.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the acceptor's id.
  logger.check(config.executorAddresses.contains(address))
  private val executorId = config.executorAddresses.indexOf(address)

  @JSExport
  protected var idToRequest: mutable.Map[UniqueId, ClientRequest] =
    mutable.Map[UniqueId, ClientRequest]()

// Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ExecutorInbound.Request
    inbound.request match {
      case Request.IdToRequest(r) => handleIdToRequest(src, r)
      case Request.ValueChosen(r) => handleValueChosen(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  def handleIdToRequest(src: Transport#Address, request: IdToRequest): Unit = {
    idToRequest.put(request.clientRequest.uniqueId, request.clientRequest)
  }

  def handleValueChosen(src: Transport#Address,
                        valueChosen: ValueChosen): Unit = {
    if (idToRequest.get(valueChosen.getUniqueId).nonEmpty) {
      val request: ClientRequest = idToRequest.get(valueChosen.getUniqueId).get

      val clientAddress = transport.addressSerializer.fromBytes(
        valueChosen.getUniqueId.clientAddress.toByteArray()
      )
      val client = chan[Client[Transport]](clientAddress, Client.serializer)
      client.send(
        ClientInbound().withClientReply(
          ClientReply(
            clientPseudonym = request.uniqueId.clientPseudonym,
            clientId = request.uniqueId.clientId,
            result = request.command
          )))
    }
  }
}

