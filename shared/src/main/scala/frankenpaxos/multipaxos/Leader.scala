package frankenpaxos.multipaxos

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
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer
}

@JSExportAll
case class LeaderOptions(
    resendPhase1asPeriod: java.time.Duration,
    // A leader flushes all of its channels to the proxy leaders after every
    // `flushPhase2asEveryN` Phase2a messages sent. For example, if
    // `flushPhase2asEveryN` is 1, then the leader flushes after every send.
    flushPhase2asEveryN: Int,
    // In Evelyn Paxos, a 100% read workload would stall, as reads require at
    // least some writes in order to be processed. Thus, a leader writes a noop
    // to the log every noopFlushPeriod. If noopFlushPeriod is 0, then no noops
    // are written.
    noopFlushPeriod: java.time.Duration,
    electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    flushPhase2asEveryN = 1,
    noopFlushPeriod = java.time.Duration.ofSeconds(0),
    electionOptions = ElectionOptions.default,
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val leaderChangesTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_leader_changes_total")
    .help("Total number of leader changes.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_resend_phase1as_total")
    .help("Total number of times the leader resent phase 1a messages.")
    .register()

  val noopsFlushedTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_leader_noops_flushed_total")
    .help("Total number of times the leader flushed a noop.")
    .register()
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.leaderAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override val serializer = LeaderInboundSerializer

  type AcceptorIndex = Int
  type Round = Int
  type Slot = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case object Inactive extends State

  @JSExportAll
  case class Phase1(
      phase1bs: mutable.Buffer[mutable.Map[AcceptorIndex, Phase1b]],
      pendingClientRequestBatches: mutable.Buffer[ClientRequestBatch],
      resendPhase1as: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase2(
      noopFlush: Option[Transport#Timer]
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  private val index = config.leaderAddresses.indexOf(address)

  // Acceptor channels.
  private val acceptors: Seq[Seq[Chan[Acceptor[Transport]]]] =
    for (acceptorCluster <- config.acceptorAddresses) yield {
      for (address <- acceptorCluster)
        yield chan[Acceptor[Transport]](address, Acceptor.serializer)
    }

  // ProxyLeader channels.
  private val proxyLeaders: Seq[Chan[ProxyLeader[Transport]]] =
    for (address <- config.proxyLeaderAddresses)
      yield chan[ProxyLeader[Transport]](address, ProxyLeader.serializer)

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // If the leader is the active leader, then this is its round. If it is
  // inactive, then this is the largest active round it knows about.
  @JSExport
  protected var round: Round = roundSystem
    .nextClassicRound(leaderIndex = 0, round = -1)

  // The next available slot in the log. Even though we have a next slot into
  // the log, you'll note that we don't even have a log! Because we've
  // decoupled aggressively, leaders don't actually need a log at all.
  @JSExport
  protected var nextSlot: Slot = 0

  // Every slot less than chosenWatermark has been chosen. Replicas
  // periodically send their chosenWatermarks to the leaders.
  @JSExport
  protected var chosenWatermark: Slot = 0

  // Leader election address. This field exists for the javascript
  // visualizations.
  @JSExport
  protected val electionAddress = config.leaderElectionAddresses(index)

  // Leader election participant.
  @JSExport
  protected val election = new Participant[Transport](
    address = electionAddress,
    transport = transport,
    logger = logger,
    addresses = config.leaderElectionAddresses,
    initialLeaderIndex = 0,
    options = options.electionOptions
  )
  election.register((leaderIndex) => {
    leaderChange(leaderIndex == index)
  })

  // The number of Phase2a messages since the last flush.
  private var numPhase2asSentSinceLastFlush: Int = 0

  // A MultiPaxos leader sends messages to ProxyLeaders one at a time.
  // `currentProxyLeader` is the proxy leader that is currently being sent to.
  // Note that this value is only used if config.distributionScheme is Hash.
  private var currentProxyLeader: Int = 0

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    startPhase1(round, chosenWatermark)
  } else {
    Inactive
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase1asTimer(
      phase1a: Phase1a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as",
      options.resendPhase1asPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        for (group <- acceptors; acceptor <- group) {
          acceptor.send(AcceptorInbound().withPhase1A(phase1a))
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeNoopFlushTimer(): Option[Transport#Timer] = {
    if (options.noopFlushPeriod == java.time.Duration.ofSeconds(0)) {
      return None
    }

    lazy val t: Transport#Timer = timer(
      s"noopFlush",
      options.noopFlushPeriod,
      () => {
        metrics.noopsFlushedTotal.inc()

        state match {
          case Inactive | _: Phase1 =>
            logger.fatal(
              s"A leader tried to flush a noop but is not in Phase 2. It's " +
                s"state is $state."
            )

          case _: Phase2 =>
        }

        getProxyLeader().send(
          ProxyLeaderInbound().withPhase2A(
            Phase2a(slot = nextSlot,
                    round = round,
                    commandBatchOrNoop = CommandBatchOrNoop().withNoop(Noop()))
          )
        )
        nextSlot += 1
        currentProxyLeader += 1
        if (currentProxyLeader >= config.numProxyLeaders) {
          currentProxyLeader = 0
        }

        t.start()
      }
    )
    t.start()
    Some(t)
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

  private def getProxyLeader(): Chan[ProxyLeader[Transport]] = {
    config.distributionScheme match {
      case Hash      => proxyLeaders(currentProxyLeader)
      case Colocated => proxyLeaders(index)
    }
  }

  private def thriftyQuorum(
      acceptors: Seq[Chan[Acceptor[Transport]]]
  ): Seq[Chan[Acceptor[Transport]]] =
    scala.util.Random.shuffle(acceptors).take(config.f + 1)

  // `maxPhase1bSlot(phase1b)` finds the largest slot present in `phase1b` or
  // -1 if no slots are present.
  private def maxPhase1bSlot(phase1b: Phase1b): Slot = {
    if (phase1b.info.isEmpty) {
      -1
    } else {
      phase1b.info.map(_.slot).max
    }
  }

  // Given a quorum of Phase1b messages, `safeValue` finds a value that is safe
  // to propose in a particular slot. If the Phase1b messages have at least one
  // vote in the slot, then the value with the highest vote round is safe.
  // Otherwise, everything is safe. In this case, we return Noop.
  private def safeValue(
      phase1bs: Iterable[Phase1b],
      slot: Slot
  ): CommandBatchOrNoop = {
    val slotInfos =
      phase1bs.flatMap(phase1b => phase1b.info.find(_.slot == slot))
    if (slotInfos.isEmpty) {
      CommandBatchOrNoop().withNoop(Noop())
    } else {
      slotInfos.maxBy(_.voteRound).voteValue
    }
  }

  private def processClientRequestBatch(
      clientRequestBatch: ClientRequestBatch
  ): Unit = {
    state match {
      case Inactive | _: Phase1 =>
        logger.fatal(
          s"A leader tried to process a client request batch but is not in " +
            s"Phase 2. It's state is $state."
        )

      case _: Phase2 =>
    }

    // Normally, we'd have the following code, but to measure the time taken
    // for serialization vs sending, we split it up. It's less readable, but it
    // leads to some better performance insights.
    //
    // getProxyLeader().send(
    //   ProxyLeaderInbound().withPhase2A(
    //     Phase2a(slot = nextSlot,
    //             round = round,
    //             commandBatchOrNoop = CommandBatchOrNoop()
    //               .withCommandBatch(clientRequestBatch.batch))
    //   )
    // )

    val proxyLeaderIndex = timed("processClientRequestBatch/getProxyLeader") {
      config.distributionScheme match {
        case Hash      => currentProxyLeader
        case Colocated => index
      }
    }

    val bytes = timed("processClientRequestBatch/serialize") {
      ProxyLeaderInbound()
        .withPhase2A(
          Phase2a(slot = nextSlot,
                  round = round,
                  commandBatchOrNoop = CommandBatchOrNoop()
                    .withCommandBatch(clientRequestBatch.batch))
        )
        .toByteArray
    }

    if (options.flushPhase2asEveryN == 1) {
      // If we flush every message, don't bother managing
      // `numPhase2asSentSinceLastFlush` or flushing channels.
      timed("processClientRequestBatch/send") {
        send(config.proxyLeaderAddresses(proxyLeaderIndex), bytes)
      }
      currentProxyLeader += 1
      if (currentProxyLeader >= config.numProxyLeaders) {
        currentProxyLeader = 0
      }
    } else {
      timed("processClientRequestBatch/sendNoFlush") {
        sendNoFlush(config.proxyLeaderAddresses(proxyLeaderIndex), bytes)
      }
      numPhase2asSentSinceLastFlush += 1
    }

    if (numPhase2asSentSinceLastFlush >= options.flushPhase2asEveryN) {
      timed("processClientRequestBatch/flush") {
        config.distributionScheme match {
          case Hash      => proxyLeaders(currentProxyLeader).flush()
          case Colocated => proxyLeaders(index).flush()
        }
      }
      numPhase2asSentSinceLastFlush = 0
      currentProxyLeader += 1
      if (currentProxyLeader >= config.numProxyLeaders) {
        currentProxyLeader = 0
      }
    }

    nextSlot += 1
  }

  private def startPhase1(round: Round, chosenWatermark: Slot): Phase1 = {
    val phase1a = Phase1a(round = round, chosenWatermark = chosenWatermark)
    for (group <- acceptors) {
      thriftyQuorum(group).foreach(
        _.send(AcceptorInbound().withPhase1A(phase1a))
      )
    }
    Phase1(
      phase1bs = mutable.Buffer.fill(config.numAcceptorGroups)(mutable.Map()),
      pendingClientRequestBatches = mutable.Buffer(),
      resendPhase1as = makeResendPhase1asTimer(phase1a)
    )
  }

  private def leaderChange(isNewLeader: Boolean): Unit = {
    metrics.leaderChangesTotal.inc()

    (state, isNewLeader) match {
      case (Inactive, false) =>
      // Do nothing.
      case (phase1: Phase1, false) =>
        phase1.resendPhase1as.stop()
        state = Inactive
      case (phase2: Phase2, false) =>
        phase2.noopFlush.foreach(_.stop())
        state = Inactive
      case (Inactive, true) =>
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
      case (phase1: Phase1, true) =>
        phase1.resendPhase1as.stop()
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
      case (phase2: Phase2, true) =>
        phase2.noopFlush.foreach(_.stop())
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import LeaderInbound.Request

    val label =
      inbound.request match {
        case Request.Phase1B(_)                  => "Phase1b"
        case Request.ClientRequest(_)            => "ClientRequest"
        case Request.ClientRequestBatch(_)       => "ClientRequestBatch"
        case Request.LeaderInfoRequestClient(_)  => "LeaderInfoRequestClient"
        case Request.LeaderInfoRequestBatcher(_) => "LeaderInfoRequestBatcher"
        case Request.Nack(_)                     => "Nack"
        case Request.ChosenWatermark(_)          => "ChosenWatermark"
        case Request.Recover(_)                  => "Recover"
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1B(r) =>
          handlePhase1b(src, r)
        case Request.ClientRequest(r) =>
          handleClientRequest(src, r)
        case Request.ClientRequestBatch(r) =>
          handleClientRequestBatch(src, r)
        case Request.LeaderInfoRequestClient(r) =>
          handleLeaderInfoRequestClient(src, r)
        case Request.LeaderInfoRequestBatcher(r) =>
          handleLeaderInfoRequestBatcher(src, r)
        case Request.Nack(r) =>
          handleNack(src, r)
        case Request.ChosenWatermark(r) =>
          handleChosenWatermark(src, r)
        case Request.Recover(r) =>
          handleRecover(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    state match {
      case Inactive | _: Phase2 =>
        logger.debug(
          s"A leader received a Phase1b message but is not in Phase1. Its " +
            s"state is $state. The Phase1b message is being ignored."
        )

      case phase1: Phase1 =>
        // Ignore messages from stale rounds.
        if (phase1b.round != round) {
          logger.debug(
            s"A leader received a Phase1b message in round ${phase1b.round} " +
              s"but is in round $round. The Phase1b is being ignored."
          )
          // If phase1b.round were larger than round, then we would have
          // received a nack instead of a Phase1b.
          logger.checkLt(phase1b.round, round)
          return
        }

        // Wait until we have a quorum of responses from _every_ acceptor group.
        phase1.phase1bs(phase1b.groupIndex)(phase1b.acceptorIndex) = phase1b
        if (phase1.phase1bs.exists(_.size < config.f + 1)) {
          return
        }

        // Find the largest slot with a vote.
        val maxSlot = phase1.phase1bs
          .map(groupPhase1bs => groupPhase1bs.values.map(maxPhase1bSlot).max)
          .max

        // Now, we iterate from chosenWatermark to maxSlot proposing safe
        // values to fill in the log.
        for (slot <- chosenWatermark to maxSlot) {
          val group = phase1.phase1bs(slot % config.numAcceptorGroups)
          getProxyLeader().send(
            ProxyLeaderInbound().withPhase2A(
              Phase2a(
                slot = slot,
                round = round,
                commandBatchOrNoop = safeValue(group.values, slot)
              )
            )
          )
        }

        // We've filled in every slot until and including maxSlot, so the next
        // slot is maxSlot + 1.
        nextSlot = maxSlot + 1

        // Update our state.
        phase1.resendPhase1as.stop()
        state = Phase2(makeNoopFlushTimer())

        // Process any pending client requests.
        for (clientRequestBatch <- phase1.pendingClientRequestBatches) {
          processClientRequestBatch(clientRequestBatch)
        }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    state match {
      case Inactive =>
        // If we're not the active leader but receive a request from a client,
        // then we send back a NotLeader message to let them know that they've
        // contacted the wrong leader.
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(ClientInbound().withNotLeaderClient(NotLeaderClient()))

      case phase1: Phase1 =>
        // We'll process the client request after we've finished phase 1.
        phase1.pendingClientRequestBatches += ClientRequestBatch(
          batch = CommandBatch(command = Seq(clientRequest.command))
        )

      case phase2: Phase2 =>
        processClientRequestBatch(
          ClientRequestBatch(
            batch = CommandBatch(command = Seq(clientRequest.command))
          )
        )
    }
  }

  private def handleClientRequestBatch(
      src: Transport#Address,
      clientRequestBatch: ClientRequestBatch
  ): Unit = {
    state match {
      case Inactive =>
        // If we're not the active leader but receive a request from a bathcer,
        // then we send back a NotLeader message to let them know that they've
        // contacted the wrong leader.
        //
        // Note that we also send back the client request batch to the batcher
        // so that it can resend it to the right leader. We don't send back
        // messages to clients because they cache their requests until they
        // receive a response.
        val batcher = chan[Batcher[Transport]](src, Batcher.serializer)
        batcher.send(
          BatcherInbound().withNotLeaderBatcher(
            NotLeaderBatcher(clientRequestBatch = clientRequestBatch)
          )
        )

      case phase1: Phase1 =>
        // We'll process the client request after we've finished phase 1.
        phase1.pendingClientRequestBatches += clientRequestBatch

      case phase2: Phase2 =>
        processClientRequestBatch(clientRequestBatch)
    }
  }

  private def handleLeaderInfoRequestClient(
      src: Transport#Address,
      leaderInfoRequest: LeaderInfoRequestClient
  ): Unit = {
    state match {
      case Inactive =>
      // We're inactive, so we ignore the leader info request. The active
      // leader will respond to the request.

      case _: Phase1 | _: Phase2 =>
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound()
            .withLeaderInfoReplyClient(LeaderInfoReplyClient(round = round))
        )
    }
  }

  private def handleLeaderInfoRequestBatcher(
      src: Transport#Address,
      leaderInfoRequest: LeaderInfoRequestBatcher
  ): Unit = {
    state match {
      case Inactive =>
      // We're inactive, so we ignore the leader info request. The active
      // leader will respond to the request.

      case _: Phase1 | _: Phase2 =>
        val batcher = chan[Batcher[Transport]](src, Batcher.serializer)
        batcher.send(
          BatcherInbound()
            .withLeaderInfoReplyBatcher(LeaderInfoReplyBatcher(round = round))
        )
    }
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    if (nack.round <= round) {
      logger.debug(
        s"A Leader received a Nack message with round ${nack.round} but is " +
          s"already in round $round. The Nack is being ignored."
      )
      return
    }

    state match {
      case Inactive =>
        // Do nothing.
        round = nack.round
      case _: Phase1 =>
        round =
          roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
        leaderChange(isNewLeader = true)
      case _: Phase2 =>
        round =
          roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
        leaderChange(isNewLeader = true)
    }
  }

  private def handleChosenWatermark(
      src: Transport#Address,
      msg: ChosenWatermark
  ): Unit = {
    chosenWatermark = Math.max(chosenWatermark, msg.slot)
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    // Note that we don't actually use `recover.slot` anywhere. This is
    // actually ok. If there is a slot in the log that needs recovering, then
    // some slot above that slot was chosen. This means that when the leader
    // runs phase 1, it will see this larger slot and recover all instances
    // below it, including `recover.slot`.
    state match {
      case Inactive =>
      // Do nothing. The active leader will recover.
      case _: Phase1 | _: Phase2 =>
        // Leader change to make sure the slot is chosen.
        leaderChange(isNewLeader = true)
    }
  }
}
