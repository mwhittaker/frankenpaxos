package frankenpaxos.simplegcbpaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object GarbageCollectorInboundSerializer
    extends ProtoSerializer[GarbageCollectorInbound] {
  type A = GarbageCollectorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class GarbageCollectorOptions()

@JSExportAll
object GarbageCollectorOptions {
  val default = GarbageCollectorOptions()
}

@JSExportAll
class GarbageCollectorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_garbage_collector_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_garbage_collector_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
object GarbageCollector {
  val serializer = GarbageCollectorInboundSerializer
}

@JSExportAll
class GarbageCollector[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: GarbageCollectorOptions = GarbageCollectorOptions.default,
    metrics: GarbageCollectorMetrics = new GarbageCollectorMetrics(
      PrometheusCollectors
    )
) extends Actor(address, transport, logger) {
  import GarbageCollector._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = GarbageCollectorInbound
  override def serializer = GarbageCollector.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration.
  logger.check(config.valid())

  // Channels to the proposers.
  private val proposers: Seq[Chan[Proposer[Transport]]] =
    for (a <- config.proposerAddresses)
      yield chan[Proposer[Transport]](a, Proposer.serializer)

  // Channels to the acceptors.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (a <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](a, Acceptor.serializer)

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: GarbageCollectorInbound
  ): Unit = {
    import GarbageCollectorInbound.Request

    val startNanos = System.nanoTime
    val label = inbound.request match {
      case Request.GarbageCollect(r) =>
        handleGarbageCollect(src, r)
        "GarbageCollect"
      case Request.Empty => {
        logger.fatal("Empty GarbageCollectorInbound encountered.")
      }
    }
    val stopNanos = System.nanoTime
    metrics.requestsTotal.labels(label).inc()
    metrics.requestsLatency
      .labels(label)
      .observe((stopNanos - startNanos).toDouble / 1000000)
  }

  private def handleGarbageCollect(
      src: Transport#Address,
      garbageCollect: GarbageCollect
  ): Unit = {
    proposers.foreach(
      _.send(ProposerInbound().withGarbageCollect(garbageCollect))
    )
    acceptors.foreach(
      _.send(AcceptorInbound().withGarbageCollect(garbageCollect))
    )
  }
}
