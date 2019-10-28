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

  // Proposer channels
  private val proposers: Seq[Chan[Proposer[Transport]]] =
    for (address <- config.proposerAddresses)
      yield chan[Proposer[Transport]](address, Proposer.serializer)

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // The round that this batcher thinks the leader is in. This value is not
  // always accurate. It's just the batcher's best guess. The leader associated
  // with this round can be computed using `roundSystem`. The batchers need to
  // know who the leader is because they need to know where to send their
  // commands.
  @JSExport
  protected var round: Int = 0

  @JSExport
  protected var growingBatch = mutable.Buffer[ClientRequest]()

  @JSExport
  protected var pendingResendBatches = mutable.Buffer[ClientRequestBatch]()

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
    import BatcherInbound.Request

    val label =
      inbound.request match {
        case Request.ClientRequest(_)          => "ClientRequest"
        case Request.NotLeaderBatcher(_)       => "NotLeaderBatcher"
        case Request.LeaderInfoReplyBatcher(_) => "LeaderInfoReplyBatcher"
        case Request.Empty =>
          logger.fatal("Empty BatcherInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r)          => handleClientRequest(src, r)
        case Request.NotLeaderBatcher(r)       => handleNotLeaderBatcher(src, r)
        case Request.LeaderInfoReplyBatcher(r) => handleLeaderInfo(src, r)
        case Request.Empty =>
          logger.fatal("Empty BatcherInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    growingBatch += clientRequest
    if (growingBatch.size >= options.batchSize) {
      val r = scala.util.Random
      val proposer = proposers(r.nextInt(config.numProposers))

      proposer.send(ProposerInbound().withRequestBatch(
        RequestBatch(growingBatch)
      ))

      growingBatch.clear()
      metrics.batchesSent.inc()
    }
  }

  private def handleNotLeaderBatcher(
      src: Transport#Address,
      notLeader: NotLeaderBatcher
  ): Unit = {
    pendingResendBatches += notLeader.clientRequestBatch
    leaders.foreach(
      _.send(
        LeaderInbound().withLeaderInfoRequestBatcher(LeaderInfoRequestBatcher())
      )
    )
  }

  private def handleLeaderInfo(
      src: Transport#Address,
      leaderInfo: LeaderInfoReplyBatcher
  ): Unit = {
    /*if (leaderInfo.round <= round) {
      logger.debug(
        s"A batcher received a LeaderInfoReplyBatcher message with round " +
          s"${leaderInfo.round} but is already in round $round. The " +
          s"LeaderInfoReplyBatcher message must be stale, so we are ignoring " +
          s"it."
      )
      return
    }

    // Update our round.
    val oldRound = round
    val newRound = leaderInfo.round
    round = leaderInfo.round

    // We've sent all of our batches to the leader of round `round`, but we
    // just learned about a new round `leaderInfo.round`. If the leader of the
    // new round is different than the leader of the old round, then we have to
    // re-send our messages.
    if (roundSystem.leader(oldRound) != roundSystem.leader(newRound)) {
      val leader = leaders(roundSystem.leader(newRound))
      for (batch <- pendingResendBatches) {
        leader.send(LeaderInbound().withClientRequestBatch(batch))
      }
    }*/
  }
}
