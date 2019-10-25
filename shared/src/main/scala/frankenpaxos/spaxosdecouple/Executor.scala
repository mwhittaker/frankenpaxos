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
import frankenpaxos.spaxosdecouple.Executor.{ECommand, ENoop, Entry}
import frankenpaxos.statemachine.StateMachine

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
  sealed trait Entry
  case class ECommand(uniqueId: UniqueId) extends Entry
  object ENoop extends Entry
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
    waitStagger: java.time.Duration,
    batchSize: Int
)

@JSExportAll
object ExecutorOptions {
  val default = ExecutorOptions(
    waitPeriod = java.time.Duration.ofMillis(25),
    waitStagger = java.time.Duration.ofMillis(25),
    batchSize = 100
  )
}

@JSExportAll
class Executor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    val stateMachine: StateMachine,
    options: ExecutorOptions = ExecutorOptions.default,
    metrics: ExecutorMetrics = new ExecutorMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  override type InboundMessage = ExecutorInbound
  override val serializer = Executor.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the acceptor's id.
  logger.check(config.executorAddresses.contains(address))
  private val executorId = config.executorAddresses.indexOf(address)

  type Slot = Int
  type ClientPseudonym = Int
  type ClientId = Int

  val log: mutable.SortedMap[Slot, Entry] = mutable.SortedMap()

  @JSExport
  protected var chosenWatermark: Slot = 0

  @JSExport
  protected var clientTable =
    mutable.Map[(Transport#Address, ClientPseudonym), (ClientId, Array[Byte])]()

  @JSExport
  protected var idRequestMap: mutable.Map[BatchId, RequestBatch] = mutable.Map()

  // Requests received from proposers to be disseminated
  @JSExport
  protected var requestsDisseminated: mutable.Set[RequestBatch] = mutable.Set()

  @JSExport
  protected var chosenIds: mutable.Set[BatchId] = mutable.Set()

  @JSExport
  protected var index: Int = config.executorAddresses.indexOf(address)

  @JSExport
  protected var chosenBeforeForward: mutable.Set[BatchId] = mutable.Set()

  private val proposers: Seq[Chan[Proposer[Transport]]] = {
    for (proposerAddress <- config.proposerAddresses)
      yield chan[Proposer[Transport]](proposerAddress, Proposer.serializer)
  }

  @JSExport
  protected var growingBatch = mutable.Buffer[Acknowledge]()

  @JSExport
  protected var pendingResendBatches = mutable.Buffer[Acknowledge]()

// Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ExecutorInbound.Request
    inbound.request match {
      case Request.ValueChosen(r) => handleValueChosen(src, r)
      case Request.ValueChosenBuffer(r) => handleValueChosenBuffer(src, r)
      case Request.Forward(r) => handleForwardRequest(src, r)
      case Request.Empty => {
        logger.fatal("Empty ExecutorInbound encountered.")
      }
    }
  }

  def handleValueChosen(src: Transport#Address,
                        valueChosen: ValueChosen): Unit = {
    /*if (idRequestMap.get(valueChosen.getUniqueId).nonEmpty && !chosenIds.contains(valueChosen.getUniqueId)) {
      val clientAddress = transport.addressSerializer.fromBytes(
        valueChosen.getUniqueId.clientAddress.toByteArray()
      )
      val client = chan[Client[Transport]](clientAddress, Client.serializer)
      client.send(
        ClientInbound().withClientReply(
          ClientReply(
            clientPseudonym = valueChosen.getUniqueId.clientPseudonym,
            clientId = valueChosen.getUniqueId.clientId,
            result = idRequestMap.get(valueChosen.getUniqueId).get
          )))
      chosenIds.add(valueChosen.getUniqueId)
    }
    val entry = valueChosen.value match {
      case ValueChosen.Value.UniqueId(command) => ECommand(command)
      case ValueChosen.Value.Noop(_)          => ENoop
      case ValueChosen.Value.Empty => null
    }

    log.get(valueChosen.slot) match {
      case Some(existingEntry) => null
      case None =>
        log(valueChosen.slot) = entry
    }

    executeLog()*/
  }

  private def handleValueChosenBuffer(
                                       src: Transport#Address,
                                       valueChosenBuffer: ValueChosenBuffer
                                     ): Unit = {
    /*metrics.requestsTotal.labels("ValueChosenBuffer").inc()
    for (valueChosen <- valueChosenBuffer.valueChosen) {
      val entry = valueChosen.value match {
        case ValueChosen.Value.UniqueId(command) => ECommand(command)
        case ValueChosen.Value.Noop(_)          => ENoop
        case ValueChosen.Value.Empty => null
      }

      log.get(valueChosen.slot) match {
        case Some(existingEntry) => null
        case None =>
          log(valueChosen.slot) = entry
      }
    }
    executeLog()*/
  }



  def handleForwardRequest(src: Transport#Address, forward: Forward) = {
    if (!requestsDisseminated.contains(forward.requestBatch)) {
      requestsDisseminated.add(forward.requestBatch)

      val id: BatchId = BatchId(java.util.UUID.randomUUID.toString)
      idRequestMap.put(id, forward.requestBatch)

      val proposer = chan[Proposer[Transport]](src, Proposer.serializer)
        proposer.send(ProposerInbound().withAcknowledge(Acknowledge(
          CommandBatch(id))))
    }
  }
}

