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

// A ReadBatcher can batch linearizable reads in three different ways:
//
//   1. _Size_. A batcher gathers reads into a batch. When the batch reaches a
//      certain size (or a timeout expires), the batch is sealed and sent.
//   2. _Time_. A batcher gathers reads into a batch. It seals and sends the
//      batch based on a timeout.
//   3. _Adaptive_. A batcher gathers reads into a batch and sends out a
//      BatchMaxSlotRequest to the acceptors. Between sending the
//      BatchMaxSlotRequest and receiving a BatchMaxSlotReply, the batcher
//      gathers reads into a new batch. When the batcher receives the
//      BatchMaxSlotReply of the previous batch, it immediately issues a
//      BatchMaxSlotRequest for the new batch.
//
// The size and time schemes can be used for all three types of reads. The
// adaptive scheme can only be used for linearizable reads.
@JSExportAll
sealed trait ReadBatchingScheme

@JSExportAll
object ReadBatchingScheme {
  case class Size(batchSize: Int, timeout: java.time.Duration)
      extends ReadBatchingScheme
  case class Time(timeout: java.time.Duration) extends ReadBatchingScheme
  case object Adaptive extends ReadBatchingScheme

  implicit val read: scopt.Read[ReadBatchingScheme] = scopt.Read.reads(s => {
    s.trim().split(",") match {
      case Array("size", batchSize, timeout) =>
        ReadBatchingScheme.Size(
          scopt.Read.intRead.reads(batchSize),
          java.time.Duration
            .ofNanos(scopt.Read.durationRead.reads(timeout).toNanos)
        )

      case Array("time", timeout) =>
        ReadBatchingScheme.Time(
          java.time.Duration
            .ofNanos(scopt.Read.durationRead.reads(timeout).toNanos)
        )

      case Array("adaptive") =>
        ReadBatchingScheme.Adaptive

      case _ =>
        throw new IllegalArgumentException(
          s"$s must look like 'size,1,1s', 'time,1s' or 'adaptive'."
        )
    }
  })
}

@JSExportAll
object ReadBatcherInboundSerializer
    extends ProtoSerializer[ReadBatcherInbound] {
  type A = ReadBatcherInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object ReadBatcher {
  val serializer = ReadBatcherInboundSerializer
}

@JSExportAll
case class ReadBatcherOptions(
    readBatchingScheme: ReadBatchingScheme,
    // If `unsafeReadAtFirstSlot` is true, all ReadRequests are issued in slot
    // 0. With this option enabled, our protocol is not safe. Reads are no
    // longer linearizable. This should be used only for performance debugging.
    unsafeReadAtFirstSlot: Boolean,
    // To perform a linearizable quorum read, a client contacts a quorum of
    // acceptors and asks them for the largest log entry in which they have
    // voted. It then computes the maximum of these log entries; let's call
    // this value i. It issues the read at i + n where n is the number of
    // acceptor groups. If `unsafeReadAtI` is true, the client instead issues
    // the read at index i. If unsafeReadAtFirstSlot is true, we instead read
    // at 0.
    unsafeReadAtI: Boolean,
    measureLatencies: Boolean
)

@JSExportAll
object ReadBatcherOptions {
  val default = ReadBatcherOptions(
    readBatchingScheme = ReadBatchingScheme.Size(
      batchSize = 100,
      timeout = java.time.Duration.ofSeconds(1)
    ),
    unsafeReadAtFirstSlot = false,
    unsafeReadAtI = false,
    measureLatencies = true
  )
}

@JSExportAll
class ReadBatcherMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_read_batcher_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val readBatchesSentTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_read_batches_sent_total")
    .help("Total number of read batches sent.")
    .register()

  val sequentialReadBatchesSentTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_sequential_read_batches_sent_total")
    .help("Total number of sequential read batches sent.")
    .register()

  val eventualReadBatchesSentTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_eventual_read_batches_sent_total")
    .help("Total number of eventual read batches sent.")
    .register()

  val linearizableTimeoutsFiredTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_linearizable_timeouts_fired_total")
    .help("Total number of linearizable timeouts fired.")
    .register()

  val sequentialTimeoutsFiredTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_sequential_timeouts_fired_total")
    .help("Total number of sequential timeouts fired.")
    .register()

  val eventualTimeoutsFiredTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_eventual_timeouts_fired_total")
    .help("Total number of eventual timeouts fired.")
    .register()

  val linearizableBatchNotFoundTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_read_batcher_linearizable_batch_not_found_total")
    .help(
      "Total number of times we receive a BatchMaxSlotReply for a batch we " +
        "don't have."
    )
    .register()

  val batchSize: Summary = collectors.summary
    .build()
    .name("multipaxos_read_batcher_batch_size")
    .labelNames("type")
    .help("Batch size.")
    .register()
}

@JSExportAll
class ReadBatcher[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ReadBatcherOptions = ReadBatcherOptions.default,
    metrics: ReadBatcherMetrics = new ReadBatcherMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReadBatcherInbound
  override val serializer = ReadBatcherInboundSerializer

  type ReadBatcherId = Int
  type AcceptorIndex = Int

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  private val index = config.readBatcherAddresses.indexOf(address)

  // Acceptor channels.
  private val acceptors: Seq[Seq[Chan[Acceptor[Transport]]]] =
    for (acceptorCluster <- config.acceptorAddresses) yield {
      for (address <- acceptorCluster)
        yield chan[Acceptor[Transport]](address, Acceptor.serializer)
    }

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (address <- config.replicaAddresses)
      yield chan[Replica[Transport]](address, Replica.serializer)

  // Linearizable reads.
  //
  // TODO(mwhittaker): We need to resend BatchMaxSlotReply requests when using
  // an adaptive batching scheme. Otherwise, if a BatchMaxSlotReply is dropped,
  // we stall.
  @JSExport
  protected var linearizableId = 0

  @JSExport
  protected var linearizableBatch = mutable.Buffer[Command]()

  // When a ReadBatcher seals a linearizable batch, it contacts acceptors for a
  // max slot. Batches hang out in `pendingLinearizableBatch` until the max
  // slot response comes back. The batches are keyed by id.
  @JSExport
  protected val pendingLinearizableBatches =
    mutable.Map[ReadBatcherId, mutable.Buffer[Command]]()

  @JSExport
  protected val batchMaxSlotReplies =
    mutable.Map[ReadBatcherId, mutable.Map[AcceptorIndex, BatchMaxSlotReply]]()

  @JSExport
  protected val linearizableTimer: Option[Transport#Timer] =
    options.readBatchingScheme match {
      case ReadBatchingScheme.Size(_, timeout) =>
        Some(makeLinearizableTimer(timeout))
      case ReadBatchingScheme.Time(timeout) =>
        Some(makeLinearizableTimer(timeout))
      case ReadBatchingScheme.Adaptive =>
        // Send a BatchMaxSlotReply to prime the pump.
        val group = acceptors(rand.nextInt(acceptors.size))
        val quorum = scala.util.Random.shuffle(group).take(config.f + 1)
        quorum.foreach(
          _.send(
            AcceptorInbound().withBatchMaxSlotRequest(
              BatchMaxSlotRequest(readBatcherIndex = index, readBatcherId = -1)
            )
          )
        )
        batchMaxSlotReplies(-1) = mutable.Map()
        None
    }

  // Sequential consistency.
  @JSExport
  protected var sequentialSlot = -1

  @JSExport
  protected val sequentialBatch = mutable.Buffer[Command]()

  @JSExport
  protected val sequentialTimer: Option[Transport#Timer] =
    options.readBatchingScheme match {
      case ReadBatchingScheme.Size(_, timeout) =>
        Some(makeSequentialTimer(timeout))
      case ReadBatchingScheme.Time(timeout) =>
        Some(makeSequentialTimer(timeout))
      case ReadBatchingScheme.Adaptive =>
        None
    }

  // Eventual consistency.
  @JSExport
  protected val eventualBatch = mutable.Buffer[Command]()

  @JSExport
  protected val eventualTimer: Option[Transport#Timer] =
    options.readBatchingScheme match {
      case ReadBatchingScheme.Size(_, timeout) =>
        Some(makeEventualTimer(timeout))
      case ReadBatchingScheme.Time(timeout) =>
        Some(makeEventualTimer(timeout))
      case ReadBatchingScheme.Adaptive =>
        None
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

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeLinearizableTimer(
      timeout: java.time.Duration
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"linearizableTimer",
      timeout,
      () => {
        metrics.linearizableTimeoutsFiredTotal.inc()

        if (linearizableBatch.size > 0) {
          // Send a BatchMaxSlotRequest to a randomly chosen acceptor group and
          // update our batch.
          val group = acceptors(rand.nextInt(acceptors.size))
          val quorum = scala.util.Random.shuffle(group).take(config.f + 1)
          quorum.foreach(
            _.send(
              AcceptorInbound().withBatchMaxSlotRequest(
                BatchMaxSlotRequest(
                  readBatcherIndex = index,
                  readBatcherId = linearizableId
                )
              )
            )
          )

          batchMaxSlotReplies(linearizableId) = mutable.Map()
          pendingLinearizableBatches(linearizableId) = linearizableBatch
          linearizableId += 1
          linearizableBatch = mutable.Buffer()
        }

        t.start()
      }
    )
    t.start()
    t
  }

  private def makeSequentialTimer(
      timeout: java.time.Duration
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"sequentialTimer",
      timeout,
      () => {
        metrics.sequentialTimeoutsFiredTotal.inc()

        // Send the batch to a randomly chosen replica and reset our batch.
        if (sequentialBatch.size > 0) {
          replicas(rand.nextInt(replicas.size)).send(
            ReplicaInbound().withSequentialReadRequestBatch(
              SequentialReadRequestBatch(
                slot = sequentialSlot,
                command = sequentialBatch.toSeq
              )
            )
          )

          metrics.sequentialReadBatchesSentTotal.inc()
          metrics.batchSize.observe(sequentialBatch.size)
          sequentialSlot = -1
          sequentialBatch.clear()
        }

        t.start()
      }
    )
    t.start()
    t
  }

  private def makeEventualTimer(
      timeout: java.time.Duration
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"eventualTimer",
      timeout,
      () => {
        metrics.eventualTimeoutsFiredTotal.inc()

        // Send the batch to a randomly chosen replica and reset our batch.
        if (eventualBatch.size > 0) {
          replicas(rand.nextInt(replicas.size)).send(
            ReplicaInbound().withEventualReadRequestBatch(
              EventualReadRequestBatch(command = eventualBatch.toSeq)
            )
          )

          metrics.eventualReadBatchesSentTotal.inc()
          metrics.batchSize.observe(eventualBatch.size)
          eventualBatch.clear()
        }

        t.start()
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ReadBatcherInbound.Request

    val label =
      inbound.request match {
        case Request.ReadRequest(_)           => "ReadRequest"
        case Request.SequentialReadRequest(_) => "SequentialReadRequest"
        case Request.EventualReadRequest(_)   => "EventualReadRequest"
        case Request.BatchMaxSlotReply(_)     => "BatchMaxSlotReply"
        case Request.Empty =>
          logger.fatal("Empty ReadBatcherInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ReadRequest(r) =>
          handleReadRequest(src, r)
        case Request.SequentialReadRequest(r) =>
          handleSequentialReadRequest(src, r)
        case Request.EventualReadRequest(r) =>
          handleEventualReadRequest(src, r)
        case Request.BatchMaxSlotReply(r) =>
          handleBatchMaxSlotReply(src, r)
        case Request.Empty =>
          logger.fatal("Empty ReadBatcherInbound encountered.")
      }
    }
  }

  private def handleReadRequest(
      src: Transport#Address,
      readRequest: ReadRequest
  ): Unit = {
    linearizableBatch += readRequest.command

    options.readBatchingScheme match {
      case ReadBatchingScheme.Size(batchSize, _) =>
        // Wait for the batch to exceed or match the batch size.
        if (linearizableBatch.size < batchSize) {
          return
        }

        // Send a BatchMaxSlotRequest to a randomly chosen acceptor group and
        // update our batch.
        val group = acceptors(rand.nextInt(acceptors.size))
        val quorum = scala.util.Random.shuffle(group).take(config.f + 1)
        quorum.foreach(
          _.send(
            AcceptorInbound().withBatchMaxSlotRequest(
              BatchMaxSlotRequest(
                readBatcherIndex = index,
                readBatcherId = linearizableId
              )
            )
          )
        )

        batchMaxSlotReplies(linearizableId) = mutable.Map()
        pendingLinearizableBatches(linearizableId) = linearizableBatch
        linearizableId += 1
        linearizableBatch = mutable.Buffer()
        linearizableTimer.foreach(_.reset())

      case _: ReadBatchingScheme.Time =>
        // Do nothing. The timer will trigger the batch.
        ()

      case ReadBatchingScheme.Adaptive =>
        // Do nothing. When we receive a BatchMaxSlotReply, we'll trigger the
        // batch.
        ()
    }
  }

  private def handleSequentialReadRequest(
      src: Transport#Address,
      readRequest: SequentialReadRequest
  ): Unit = {
    sequentialSlot = Math.max(sequentialSlot, readRequest.slot)
    sequentialBatch += readRequest.command

    options.readBatchingScheme match {
      case ReadBatchingScheme.Size(batchSize, _) =>
        // Wait for the batch to exceed or match the batch size.
        if (sequentialBatch.size < batchSize) {
          return
        }

        // Send the batch to a randomly chosen replica and reset our batch.
        replicas(rand.nextInt(replicas.size)).send(
          ReplicaInbound().withSequentialReadRequestBatch(
            SequentialReadRequestBatch(
              slot = sequentialSlot,
              command = sequentialBatch.toSeq
            )
          )
        )

        metrics.sequentialReadBatchesSentTotal.inc()
        metrics.batchSize.observe(sequentialBatch.size)
        sequentialSlot = -1
        sequentialBatch.clear()
        sequentialTimer.foreach(_.reset())

      case _: ReadBatchingScheme.Time =>
        // Do nothing. The timer will trigger the batch.
        ()

      case ReadBatchingScheme.Adaptive =>
        logger.fatal(
          "An adaptive read batching scheme cannot be used with sequentially " +
            "consistent reads."
        )
    }
  }

  private def handleEventualReadRequest(
      src: Transport#Address,
      readRequest: EventualReadRequest
  ): Unit = {
    eventualBatch += readRequest.command

    options.readBatchingScheme match {
      case ReadBatchingScheme.Size(batchSize, _) =>
        // Wait for the batch to exceed or match the batch size.
        if (eventualBatch.size < batchSize) {
          return
        }

        // Send the batch to a randomly chosen replica and reset our batch.
        replicas(rand.nextInt(replicas.size)).send(
          ReplicaInbound().withEventualReadRequestBatch(
            EventualReadRequestBatch(command = eventualBatch.toSeq)
          )
        )

        metrics.eventualReadBatchesSentTotal.inc()
        metrics.batchSize.observe(eventualBatch.size)
        eventualBatch.clear()
        eventualTimer.foreach(_.reset())

      case _: ReadBatchingScheme.Time =>
        // Do nothing. The timer will trigger the batch.
        ()

      case ReadBatchingScheme.Adaptive =>
        logger.fatal(
          "An adaptive read batching scheme cannot be used with eventually " +
            "consistent reads."
        )
    }
  }

  private def handleBatchMaxSlotReply(
      src: Transport#Address,
      batchMaxSlotReply: BatchMaxSlotReply
  ): Unit = {
    val slot = batchMaxSlotReplies.get(batchMaxSlotReply.readBatcherId) match {
      case None =>
        logger.debug(
          s"ReadBatcher received a batchMaxSlotReply for id " +
            s"${batchMaxSlotReply.readBatcherId} but doesn't have any " +
            s"batchMaxSlotReplies entry for it. A batchMaxSlotReply may have " +
            s"been duplicated. We are ignoring it."
        )
        return

      case Some(replies) =>
        // Wait until we have f+1 responses.
        replies(batchMaxSlotReply.acceptorIndex) = batchMaxSlotReply
        if (replies.size < config.f + 1) {
          return
        }

        // Compute the slot.
        //
        // TODO(mwhittaker): Double check that the `- 1` is safe.
        val slot = if (options.unsafeReadAtFirstSlot) {
          0
        } else if (options.unsafeReadAtI) {
          replies.values.map(_.slot).max
        } else {
          replies.values.map(_.slot).max + acceptors.size - 1
        }

        // Clean up metadata.
        batchMaxSlotReplies.remove(batchMaxSlotReply.readBatcherId)

        slot
    }

    pendingLinearizableBatches.get(batchMaxSlotReply.readBatcherId) match {
      case None =>
        // We may not have a batch because (a) we've already received the
        // BatchMaxSlotReply or (b) because we're using an adaptive batching
        // scheme and we're receiving the initial primer response.
        metrics.linearizableBatchNotFoundTotal.inc()

      case Some(batch) =>
        replicas(rand.nextInt(replicas.size)).send(
          ReplicaInbound().withReadRequestBatch(
            ReadRequestBatch(slot = slot, command = batch.toSeq)
          )
        )

        metrics.readBatchesSentTotal.inc()
        metrics.batchSize.observe(batch.size)
        pendingLinearizableBatches.remove(batchMaxSlotReply.readBatcherId)
    }

    options.readBatchingScheme match {
      case _: ReadBatchingScheme.Size | _: ReadBatchingScheme.Time =>
        // Do nothing.
        ()

      case ReadBatchingScheme.Adaptive =>
        // Send a BatchMaxSlotRequest to a randomly chosen acceptor group and
        // update our batch.
        val group = acceptors(rand.nextInt(acceptors.size))
        val quorum = scala.util.Random.shuffle(group).take(config.f + 1)
        quorum.foreach(
          _.send(
            AcceptorInbound().withBatchMaxSlotRequest(
              BatchMaxSlotRequest(
                readBatcherIndex = index,
                readBatcherId = linearizableId
              )
            )
          )
        )

        batchMaxSlotReplies(linearizableId) = mutable.Map()
        if (linearizableBatch.size > 0) {
          pendingLinearizableBatches(linearizableId) = linearizableBatch
        }
        linearizableId += 1
        linearizableBatch = mutable.Buffer()
    }
  }
}
