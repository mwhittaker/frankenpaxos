package frankenpaxos.scalog

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
import frankenpaxos.util
import scala.scalajs.js.annotation._

@JSExportAll
object AggregatorInboundSerializer extends ProtoSerializer[AggregatorInbound] {
  type A = AggregatorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Aggregator {
  val serializer = AggregatorInboundSerializer
}

@JSExportAll
case class AggregatorOptions(
    // The aggregator periodically receives shard cuts from servers. The
    // aggregator periodically aggregates these shard cuts into a global cut
    // and proposes the global cut to the Paxos leader. The aggregator will
    // propose a global cut after every `numShardCutsPerProposal` shard cuts
    // that it receives.
    numShardCutsPerProposal: Int,
    // If the aggregator has a hole in its log of cuts for more than
    // `recoverPeriod`, it polls the Paxos leader to fill it.
    recoverPeriod: java.time.Duration,
    // The aggregator sends leaderInfo requests every `leaderInfoPeriod`.
    leaderInfoPeriod: java.time.Duration,
    // The aggregator implements its log of raw cuts as a BufferMap. This is
    // the BufferMap's `logGrowSize`.
    logGrowSize: Int,
    // If `unsafeDontRecover` is true, the aggregator doesn't make any attempt
    // to recover cuts. This is not live and should only be used for
    // performance debugging.
    unsafeDontRecover: Boolean,
    // Whether or not we should measure the latency of processing every request.
    measureLatencies: Boolean
)

@JSExportAll
object AggregatorOptions {
  val default = AggregatorOptions(
    numShardCutsPerProposal = 2,
    recoverPeriod = java.time.Duration.ofSeconds(1),
    leaderInfoPeriod = java.time.Duration.ofSeconds(1),
    logGrowSize = 5000,
    unsafeDontRecover = false,
    measureLatencies = true
  )
}

@JSExportAll
class AggregatorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("scalog_aggregator_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("scalog_aggregator_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val proposalsSent: Counter = collectors.counter
    .build()
    .name("scalog_aggregator_proposals_sent")
    .help("Total number of proposals sent.")
    .register()

  val rawCutsPruned: Counter = collectors.counter
    .build()
    .name("scalog_aggregator_raw_cuts_pruned")
    .help(
      "Total number of raw cuts that the aggregator prunes because it does " +
        "not obey the monotonic order of cuts."
    )
    .register()

  val recoversSent: Counter = collectors.counter
    .build()
    .name("scalog_aggregator_recovers_sent")
    .help("Total number of Recovers sent.")
    .register()
}

@JSExportAll
class Aggregator[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: AggregatorOptions = AggregatorOptions.default,
    metrics: AggregatorMetrics = new AggregatorMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.aggregatorAddress == address)

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = AggregatorInbound
  override val serializer = AggregatorInboundSerializer

  type Slot = Int
  type Nonce = Int
  type Cut = Seq[Slot]

  // Fields ////////////////////////////////////////////////////////////////////
  // Server channels.
  private val servers: Seq[Chan[Server[Transport]]] =
    for (shard <- config.serverAddresses; a <- shard)
      yield chan[Server[Transport]](a, Server.serializer)

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // The round system used by the leaders.
  private val roundSystem =
    new RoundSystem.ClassicRoundRobin(config.leaderAddresses.size)

  // The largest round we know of. roundSystem.leader(round) is who we think is
  // the current active leader.
  @JSExport
  protected var round = 0

  // Imagine we have a Scalog deployment with three shards with servers [a, b],
  // [c, d], and [e, f, g]. Then, shardCuts looks something like the following.
  //
  //      +----------+
  //      | +------+ |
  //      | |[1, 1]| |
  //    0 | +------+ |
  //      | |[0, 2]| |
  //      | +------+ |
  //      +----------+
  //      | +------+ |
  //      | |[2, 4]| |
  //    1 | +------+ |
  //      | |[2, 2]| |
  //      | +------+ |
  //      +----------+
  //      | +------+ |
  //      | |[4, 3]| |
  //      | +------+ |
  //    2 | |[2, 1]| |
  //      | +------+ |
  //      | |[0, 1]| |
  //      | +------+ |
  //      +----------+
  //
  // We would collapse this into the global cut [1, 2, 2, 4, 4, 3].
  @JSExport
  protected val shardCuts: mutable.Buffer[mutable.Buffer[Cut]] =
    config.serverAddresses
      .map(shard => {
        shard.map(foo => Seq.fill(shard.size)(0)).to[mutable.Buffer]
      })
      .to[mutable.Buffer]

  @JSExport
  protected var numShardCutsSinceLastProposal: Int = 0

  // The log of raw cuts decided by Paxos. cuts is a pruned version of rawCuts
  // that is guaranteed to have monotonically increasing cuts. For example:
  //
  //               0   1   2   3   4
  //             +---+---+---+---+---+---
  //     rawCuts |0,0|1,2|2,1|2,2|1,1| ...
  //             +---+---+---+---+---+---
  //             +---+---+---+---+---+---
  //        cuts |0,0|1,2|2,2|   |   | ...
  //             +---+---+---+---+---+---
  //
  // Note that raw cut 2 was pruned because it is not monotonically larger than
  // raw cut 1. Similarly, raw cut 4 was pruned.
  //
  // How is it possible for rawCuts not be monotonically increasing? The raw
  // cuts that the aggregator proposes are monotonically increasing, but they
  // may arrive out of order at the Paxos leader and may be ordered in
  // non-monontically increasing order.
  @JSExport
  protected val rawCuts: util.BufferMap[GlobalCutOrNoop] =
    new util.BufferMap(options.logGrowSize)

  @JSExport
  protected val cuts: mutable.Buffer[Cut] = mutable.Buffer()

  // Every log entry < rawCutsWatermark is chosen in rawCuts. Entry
  // rawCutsWatermark is not chosen.
  @JSExport
  protected var rawCutsWatermark: Int = 0

  // The number of entries in rawCuts.
  @JSExport
  protected var numRawCutsChosen: Int = 0

  @JSExport
  protected val recoverTimer: Option[Transport#Timer] =
    if (options.unsafeDontRecover) {
      None
    } else {
      lazy val t: Transport#Timer = timer(
        s"recoverTimer",
        options.recoverPeriod,
        () => {
          metrics.recoversSent.inc()
          leaders(roundSystem.leader(round)).send(
            LeaderInbound().withRecover(Recover(slot = rawCutsWatermark))
          )
          t.start()
        }
      )
      Some(t)
    }

  @JSExport
  protected val leaderInfoTimer: Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"leaderInfoTimer",
      options.leaderInfoPeriod,
      () => {
        for (leader <- leaders) {
          leader.send(
            LeaderInbound().withLeaderInfoRequest(LeaderInfoRequest())
          )
        }
        t.start()
      }
    )
    t.start()
    t
  }

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

  private def pairwiseMax(xs: Seq[Int], ys: Seq[Int]): Seq[Int] = {
    require(xs.size == ys.size)
    xs.zip(ys).map({ case (x, y) => Math.max(x, y) })
  }

  private def pairwiseMax(seqs: Seq[Seq[Int]]): Seq[Int] = {
    require(seqs.size > 0)
    seqs.reduce(pairwiseMax)
  }

  private def monotonicallyLt(xs: Seq[Int], ys: Seq[Int]): Boolean = {
    require(xs.size == ys.size)
    xs != ys && xs.zip(ys).forall({ case (x, y) => x <= y })
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import AggregatorInbound.Request

    val label =
      inbound.request match {
        case Request.ShardInfo(_)       => "ShardInfo"
        case Request.RawCutChosen(_)    => "RawCutChosen"
        case Request.LeaderInfoReply(_) => "LeaderInfoReply"
        case Request.Recover(_)         => "Recover"
        case Request.Empty =>
          logger.fatal("Empty AggregatorInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ShardInfo(r)       => handleShardInfo(src, r)
        case Request.RawCutChosen(r)    => handleRawCutChosen(src, r)
        case Request.LeaderInfoReply(r) => handleLeaderInfoReply(src, r)
        case Request.Recover(r)         => handleRecover(src, r)
        case Request.Empty =>
          logger.fatal("Empty AggregatorInbound encountered.")
      }
    }
  }

  private def handleShardInfo(
      src: Transport#Address,
      shardInfo: ShardInfo
  ): Unit = {
    // Update the server's shard cut.
    shardCuts(shardInfo.shardIndex)(shardInfo.serverIndex) = pairwiseMax(
      shardCuts(shardInfo.shardIndex)(shardInfo.serverIndex),
      shardInfo.watermark
    )

    // We send a proposal every numShardCutsPerProposal cuts.
    numShardCutsSinceLastProposal += 1
    if (numShardCutsSinceLastProposal >= options.numShardCutsPerProposal) {
      leaders(roundSystem.leader(round)).send(
        LeaderInbound().withProposeCut(
          ProposeCut(GlobalCut(shardCuts.map(pairwiseMax).flatten))
        )
      )
      numShardCutsSinceLastProposal = 0
      metrics.proposalsSent.inc()
    }
  }

  private def handleRawCutChosen(
      src: Transport#Address,
      rawCutChosen: RawCutChosen
  ): Unit = {
    // Ignore redundantly chosen raw cuts.
    if (rawCuts.get(rawCutChosen.slot).isDefined) {
      return
    }

    // If `numRawCutsChosen != rawCutsWatermark`, then the recover timer is running.
    val isRecoverTimerRunning = numRawCutsChosen != rawCutsWatermark
    val oldRawCutsWatermark = rawCutsWatermark

    rawCuts.put(rawCutChosen.slot, rawCutChosen.rawCutOrNoop)
    numRawCutsChosen += 1

    while (rawCuts.get(rawCutsWatermark).isDefined) {
      rawCuts.get(rawCutsWatermark).map(_.value) match {
        case None =>
          logger.fatal("Unreachable code.")

        case Some(GlobalCutOrNoop.Value.Noop(Noop())) =>
          // Do nothing.
          ()

        case Some(GlobalCutOrNoop.Value.GlobalCut(globalCut)) =>
          val cut = globalCut.watermark
          if (cuts.isEmpty || monotonicallyLt(cuts.last, cut)) {
            val slot = cuts.size
            cuts += cut
            for (server <- servers) {
              server.send(
                ServerInbound().withCutChosen(
                  CutChosen(slot = slot, cut = GlobalCut(watermark = cut))
                )
              )
            }
          } else {
            metrics.rawCutsPruned.inc()
          }

        case Some(GlobalCutOrNoop.Value.Empty) =>
          logger.fatal("Empty GlobalCutOrNoop encountered.")
      }

      rawCutsWatermark += 1
    }

    val shouldRecoverTimerBeRunning = numRawCutsChosen != rawCutsWatermark
    val shouldRecoverTimerBeReset = oldRawCutsWatermark != rawCutsWatermark
    if (options.unsafeDontRecover) {
      // Do nothing.
    } else if (isRecoverTimerRunning) {
      (shouldRecoverTimerBeRunning, shouldRecoverTimerBeReset) match {
        case (true, true)  => recoverTimer.foreach(_.reset())
        case (true, false) => // Do nothing.
        case (false, _)    => recoverTimer.foreach(_.stop())
      }
    } else if (shouldRecoverTimerBeRunning) {
      recoverTimer.foreach(_.start())
    }
  }

  private def handleLeaderInfoReply(
      src: Transport#Address,
      leaderInfoReply: LeaderInfoReply
  ): Unit = {
    round = Math.max(round, leaderInfoReply.round)
  }

  // TODO(mwhittaker): Extract logic and add unit test.
  private def handleRecover(src: Transport#Address, recover: Recover): Unit = {
    // If a replica has a hole in its log for too long, it sends us a recover
    // message. We have to figure out which server owns this slot and what is
    // the corresponding cut. See `projectCut` inside of Server for a picture
    // to reference when thinking about this translation.
    //
    // TODO(mwhittaker): For now, we implement this as a linear search. This
    // performs increasingly poorly over time as the number of cuts increases.
    // We can implement binary search to implement this much faster.

    var start = 0
    var stop = 0
    for ((cut, i) <- cuts.zipWithIndex) {
      stop = cut.sum
      if (start <= recover.slot && recover.slot < stop) {
        val previousCut = if (recover.slot == 0) {
          cut.map(_ => 0)
        } else {
          cuts(recover.slot - 1)
        }
        val diffs = cut.zip(previousCut).map({ case (x, y) => x - y })
        stop = start
        for ((diff, j) <- diffs.zipWithIndex) {
          stop += diff
          if (start <= recover.slot && recover.slot < stop) {
            servers(j).send(
              ServerInbound()
                .withCutChosen(CutChosen(slot = i, cut = GlobalCut(cut)))
            )
            return
          }
          start = stop
        }
      }
      start = stop
    }

    // We didn't find a corresponding cut. We ignore the recover.
  }
}
