package frankenpaxos.spaxosdecouple

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

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
case class DisseminatorOptions(
    measureLatencies: Boolean
)

@JSExportAll
object DisseminatorOptions {
  val default = DisseminatorOptions(
    measureLatencies = true
  )
}

@JSExportAll
class DisseminatorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("spaxosdecouple_disseminator_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("spaxosdecouple_disseminator_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
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
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = DisseminatorInbound
  override val serializer = DisseminatorInboundSerializer

  type Slot = Int

  @JSExportAll
  case class State(
      voteRound: Int,
      voteValue: CommandBatchOrNoop
  )

  // Fields ////////////////////////////////////////////////////////////////////
  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (address <- config.leaderAddresses)
      yield chan[Leader[Transport]](address, Leader.serializer)

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (address <- config.replicaAddresses)
      yield chan[Replica[Transport]](address, Replica.serializer)

  private val disseminators: Seq[Seq[Chan[Disseminator[Transport]]]] =
    for (disseminatorCluster <- config.disseminatorAddresses) yield {
      for (address <- disseminatorCluster)
        yield chan[Disseminator[Transport]](address, Disseminator.serializer)
    }

  // Disseminator index.
  private val groupIndex =
    config.disseminatorAddresses.indexWhere(_.contains(address))
  private val index =
    config.disseminatorAddresses(groupIndex).indexOf(address)

  @JSExport
  protected var idRequestMap: mutable.Map[BatchId, RequestBatch] = mutable.Map()

  @JSExport
  protected var requestsDisseminated: mutable.Set[RequestBatch] = mutable.Set()

  // Helpers ///////////////////////////////////////////////////////////////////
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
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import DisseminatorInbound.Request

    val label =
      inbound.request match {
        case Request.ValueChosen(_) => "ValueChosen"
        case Request.Forward(_) => "Forward"
        case Request.Empty =>
          logger.fatal("Empty DisseminatorInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ValueChosen(r) => handleValueChosen(src, r)
        case Request.Forward(r) => handleForward(src, r)
        case Request.Empty =>
          logger.fatal("Empty DisseminatorInbound encountered.")
      }
    }
  }

  private def handleValueChosen(
      src: Transport#Address,
      valueChosen: ValueChosen
  ): Unit = {
    var requestBatch: RequestBatchOrNoop = null

      valueChosen.commandBatchOrNoop.value match {
      case CommandBatchOrNoop.Value.CommandBatch(batch) =>
        if (idRequestMap.contains(batch.batchId)) {
          requestBatch = RequestBatchOrNoop(
            RequestBatchOrNoop.Value.CommandBatch(idRequestMap.get(batch.batchId).get))
        } else {
          // This disseminator does not have the batch send the chosen command to another disseminator
          val nextIndex = (index + 1) % disseminators(groupIndex).size
          disseminators(groupIndex)(nextIndex).send(DisseminatorInbound().withValueChosen(valueChosen))
        }

      case CommandBatchOrNoop.Value.Noop(Noop()) => requestBatch = RequestBatchOrNoop(RequestBatchOrNoop.Value.Noop(Noop()))
      case CommandBatchOrNoop.Value.Empty =>
        logger.fatal("Empty CommandBatchOrNoop encountered.")
    }

    // If this disseminator had the batch notify the replicas and send the actual batch of commands
    if (requestBatch != null) {
      for (replica <- replicas) {
        replica.send(ReplicaInbound().withChosen(Chosen(
          slot = valueChosen.slot,
          requestBatch
        )))
      }
    }

  }

  private def handleForward(
      src: Transport#Address,
      forward: Forward
  ): Unit = {
    //if (!requestsDisseminated.contains(forward.requestBatch)) {
      requestsDisseminated.add(forward.requestBatch)

      val id: BatchId = forward.batchId
      idRequestMap.put(id, forward.requestBatch)

      val proposer = chan[Proposer[Transport]](src, Proposer.serializer)
      proposer.send(ProposerInbound().withAcknowledge(Acknowledge(
        CommandBatch(id))))
    //}
  }
}
