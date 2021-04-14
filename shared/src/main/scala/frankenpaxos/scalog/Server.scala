package frankenpaxos.scalog

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.election.basic.Participant
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.quorums.Grid
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.util
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object ServerInboundSerializer extends ProtoSerializer[ServerInbound] {
  type A = ServerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Server {
  val serializer = ServerInboundSerializer
}

@JSExportAll
case class ServerOptions(
    // A server periodically pushes its shard cut to the aggregrator.
    // `pushPeriod` determines how often the server performs this push.
    pushPeriod: java.time.Duration,
    // A server acts as a backup for the other servers in its shard. If the
    // server has a hole in a log that its backing up for more than
    // `recoverPeriod`, it polls the primary to fill it.
    recoverPeriod: java.time.Duration,
    // Every server is the primary of one log and a backup of one log for every
    // other server in its shard. We implement the log as a BufferMap. This is
    // the BufferMap's `logGrowSize`.
    logGrowSize: Int,
    // If `unsafeDontRecover` is true, servers don't make any attempt to
    // recover log entries. This is not live and should only be used for
    // performance debugging.
    unsafeDontRecover: Boolean,
    // Whether or not we should measure the latency of processing every request.
    measureLatencies: Boolean
)

@JSExportAll
object ServerOptions {
  val default = ServerOptions(
    pushPeriod = java.time.Duration.ofMillis(100),
    recoverPeriod = java.time.Duration.ofSeconds(1),
    logGrowSize = 5000,
    unsafeDontRecover = false,
    measureLatencies = true
  )
}

@JSExportAll
class ServerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("scalog_server_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("scalog_server_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val pushesSent: Counter = collectors.counter
    .build()
    .name("scalog_server_pushes_sent")
    .help("Total number of pushes sent.")
    .register()

  val recoversSent: Counter = collectors.counter
    .build()
    .name("scalog_server_recovers_sent")
    .help("Total number of Recovers sent.")
    .register()
}

@JSExportAll
class Server[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ServerOptions = ServerOptions.default,
    metrics: ServerMetrics = new ServerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.serverAddresses.exists(_.contains(address)))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ServerInbound
  override val serializer = ServerInboundSerializer

  type ShardIndex = Int
  type ServerIndex = Int
  type Slot = Int

  // Fields ////////////////////////////////////////////////////////////////////
  private val shardIndex: ShardIndex =
    config.serverAddresses.indexWhere(_.contains(address))
  private val index: ServerIndex =
    config.serverAddresses(shardIndex).indexOf(address)

  // Server channels.
  private val servers: Seq[Chan[Server[Transport]]] =
    for (a <- config.serverAddresses(shardIndex))
      yield chan[Server[Transport]](a, Server.serializer)

  private val otherServers: Seq[Chan[Server[Transport]]] =
    for ((c, i) <- servers.zipWithIndex if i != index) yield c

  // Aggregator channel.
  val aggregator =
    chan[Aggregator[Transport]](config.aggregatorAddress, Acceptor.serializer)

  @JSExportAll
  class Log(i: ShardIndex) {
    // The log for which a server is primary won't have any holes in it, but
    // the logs for which a server is a backup may have holes in it, hence the
    // BufferMap instead of something simpler like a Buffer.
    val log: util.BufferMap[Command] = new util.BufferMap(options.logGrowSize)

    // `watermark` is the largest integer w such that `log` contains an entry
    // in every index < w.
    var watermark: Slot = 0

    // The number of commands in `log`. If `numCommands != watermark`, there is
    // a hole.
    var numCommands: Int = 0

    // Whenever there is a hole in the log, recoverTimer is running. If the
    // timer expires, a recover message is sent to the primary to try and
    // recover the hole. Note that if we're the primary of the log, we don't
    // need a recovery timer.
    val recoverTimer: Option[Transport#Timer] = if (index == i) {
      None
    } else {
      lazy val t: Transport#Timer = timer(
        s"recoverTimer${i}",
        options.recoverPeriod,
        () => {
          metrics.recoversSent.inc()
          servers(i).send(
            ServerInbound().withRecover(Recover(slot = watermark))
          )
          t.start()
        }
      )
      Some(t)
    }

    def put(index: Slot, command: Command): Unit = {
      // If `numCommands != watermark`, then the recover timer is running.
      val isRecoverTimerRunning = numCommands != watermark
      val oldWatermark = watermark

      log.put(index, command)
      while (log.get(watermark).isDefined) {
        watermark += 1
      }

      // The recover timer should be running if there is a hole. If the recover
      // timer should be running, it's either for a hole for which the timer was
      // already running (shouldRecoverTimerBeReset = false) or for a new hole
      // for which the timer wasn't yet running (shouldRecoverTimerBeReset =
      // true).
      val shouldRecoverTimerBeRunning = numCommands != watermark
      val shouldRecoverTimerBeReset = oldWatermark != watermark
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
  }

  // If there are n servers in every shard, then logs has n logs in it. This
  // server is the primary for logs[index] and is a backup for all the other
  // logs.
  @JSExportAll
  protected val logs: mutable.Buffer[Log] =
    mutable.Buffer.tabulate(servers.size)(new Log(_))

  @JSExportAll
  protected val pushTimer: Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"pushTimer",
      options.pushPeriod,
      () => {
        metrics.pushesSent.inc()
        aggregator.send(
          AggregatorInbound().withShardInfo(
            ShardInfo(shard_index = shardIndex,
                      server_index = index,
                      watermark = logs.map(_.watermark))
          )
        )
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

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ServerInbound.Request

    val label =
      inbound.request match {
        case Request.ClientRequest(_) => "ClientRequest"
        case Request.Backup(_)        => "Backup"
        case Request.CutChosen(_)     => "CutChosen"
        case Request.Recover(_)       => "Recover"
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r) => handleClientRequest(src, r)
        case Request.Backup(r)        => handleBackup(src, r)
        case Request.CutChosen(r)     => handleCutChosen(src, r)
        case Request.Recover(r)       => handleRecover(src, r)
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleBackup(
      src: Transport#Address,
      backup: Backup
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleCutChosen(
      src: Transport#Address,
      cutChosen: CutChosen
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    // TODO(mwhittaker): Implement.
    ???
  }
}
