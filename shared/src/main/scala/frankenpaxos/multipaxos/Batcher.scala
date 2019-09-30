package frankenpaxos.multipaxos

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
object BatcherInboundSerializer extends ProtoSerializer[BatcherInbound] {
  type A = BatcherInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Batcher {
  val serializer = BatcherInboundSerializer
}

@JSExportAll
case class BatcherOptions(
    batchSize: Int,
    measureLatencies: Boolean
)

@JSExportAll
object BatcherOptions {
  val default = BatcherOptions(
    batchSize = 100,
    measureLatencies = true
  )
}

@JSExportAll
class BatcherMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_batcher_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_batcher_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val batchesSent: Counter = collectors.counter
    .build()
    .name("multipaxos_batcher_batches_sent")
    .labelNames("type")
    .help("Total number of batches sent.")
    .register()
}

@JSExportAll
class Batcher[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: BatcherOptions = BatcherOptions.default,
    metrics: BatcherMetrics = new BatcherMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = BatcherInbound
  override val serializer = BatcherInboundSerializer

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (address <- config.leaderAddresses)
      yield chan[Leader[Transport]](address, Leader.serializer)

  // The round that this batcher thinks the leader is in. This value is not
  // always accurate. It's just the batcher's best guess. The leader associated
  // with this round can be computed using `config.roundSystem`. The batchers
  // need to know who the leader is because they need to know where to send
  // their commands.
  @JSExport
  protected var round: Int = 0

  @JSExport
  protected var pendingBatch = mutable.Buffer[Command]()

  // Methods ///////////////////////////////////////////////////////////////////
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
    import BatcherInbound.Request

    val label =
      inbound.request match {
        case Request.ClientRequest(r) => "ClientRequest"
        case Request.LeaderInfo(r)    => "LeaderInfo"
        case Request.Empty =>
          logger.fatal("Empty BatcherInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r) => handleClientRequest(src, r)
        case Request.LeaderInfo(r)    => handleLeaderInfo(src, r)
        case Request.Empty =>
          logger.fatal("Empty BatcherInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    pendingBatch += clientRequest.command
    if (pendingBatch.size >= options.batchSize) {
      val leader = leaders(config.roundSystem.leader(round))
      leader.send(
        LeaderInbound().withClientRequestBatch(
          ClientRequestBatch(batch = CommandBatch(command = pendingBatch.toSeq))
        )
      )
      pendingBatch.clear()
      metrics.batchesSent.inc()
    }
  }

  private def handleLeaderInfo(
      src: Transport#Address,
      leaderInfo: LeaderInfo
  ): Unit = {
    if (leaderInfo.round <= round) {
      logger.debug(
        s"A batcher received a LeaderInfo message with round " +
          s"${leaderInfo.round} but is already in round $round. The " +
          s"LeaderInfo message must be stale, so we are ignoring it."
      )
      return
    }

    round = leaderInfo.round
  }
}
