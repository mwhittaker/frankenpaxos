package frankenpaxos.vanillamencius

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.election.basic.Participant
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
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
    // Mencius round-robin partitions the log among a set of leaders. If one
    // leader executes faster than the others, then there can be large gaps in
    // the log. To prevent this, every once in a while, a leader broadcasts its
    // nextSlot to all other leaders. If a leader's nextSlot is significantly
    // lagging the global maximum nextSlot, then we issue a range of noops. The
    // maximum nextSlot among all leader groups is called a high watermark.
    //
    // The leader broadcasts its nextSlot (indirectly through the proxy
    // leaders) every `sendHighWatermarkEveryN` commands.
    sendHighWatermarkEveryN: Int,
    // Whenever a leader updates its high watermark, if its nextSlot lags by
    // `sendNoopRangeIfLaggingBy` or more, then it issues a range of noops to
    // fill in the missing slots. If the leader is lagging but not by much,
    // then it doesn't send noops.
    sendNoopRangeIfLaggingBy: Int,
    resendPhase1asPeriod: java.time.Duration,
    // A leader flushes all of its channels to the proxy leaders after every
    // `flushPhase2asEveryN` Phase2a messages sent. For example, if
    // `flushPhase2asEveryN` is 1, then the leader flushes after every send.
    flushPhase2asEveryN: Int,
    electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    sendHighWatermarkEveryN = 10000,
    sendNoopRangeIfLaggingBy = 10000,
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    flushPhase2asEveryN = 1,
    electionOptions = ElectionOptions.default,
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("vanilla_mencius_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val nextSlot: Gauge = collectors.gauge
    .build()
    .name("vanilla_mencius_leader_next_slot")
    .help("The leader's nextSlot.")
    .register()

  val highWatermark: Gauge = collectors.gauge
    .build()
    .name("vanilla_mencius_leader_high_watermark")
    .help("The leader's highWatermark.")
    .register()

  val chosenWatermark: Gauge = collectors.gauge
    .build()
    .name("vanilla_mencius_leader_chosen_watermark")
    .help("The leader's chosenWatermark.")
    .register()

  val highWatermarksSentTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_leader_high_watermarks_sent_total")
    .help("Total number of high watermarks sent.")
    .register()

  val noopRangesSentTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_leader_noop_ranges_sent_total")
    .help("Total number of noop ranges sent as a result of lagging nextSlot.")
    .register()

  val leaderChangesTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_leader_leader_changes_total")
    .help("Total number of leader changes.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_leader_resend_phase1as_total")
    .help("Total number of times the leader resent phase 1a messages.")
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

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override val serializer = LeaderInboundSerializer

  type AcceptorIndex = Int
  type Round = Int
  type Slot = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case class Phase1(
      round: Round,
      phase1bs: mutable.Map[AcceptorIndex, Phase1b],
      resendPhase1as: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase2(
      round: Round,
      phase2bs: mutable.Map[AcceptorIndex, Phase2b]
  ) extends State

  @JSExportAll
  case class PendingEntry(
      voteRound: Round,
      voteValue: CommandOrNoop
  ) extends State

  @JSExportAll
  case class Chosen(
      value: CommandOrNoop
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // log, probably like what FasterPaxos has, but with a round number in some
  // of the log entries for when we hit a phase1a. maybe a state per log entry
  // since we can kinda be in different states in different spots in the log
  //
  // we're always in round 0 of our log, except that we're sometimes in larger
  // rounds during recovery.
  //
  // get client request:
  //   update log
  //   send phase2as
  //   send skips too if needed
  //
  // get phase2a:
  //   vote and send back
  //   update index + skips
  //
  // on timer:
  //   send skips if needed to everyone
  //
  // on failure:
  //   somehow pick a dude to recover
  //   send phase1as

  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  @JSExport
  protected val groupIndex =
    config.leaderAddresses.indexWhere(_.contains(address))

  @JSExport
  protected val index = config.leaderAddresses(groupIndex).indexOf(address)

  // Acceptor channels.
  private val acceptors: Seq[Seq[Chan[Acceptor[Transport]]]] =
    for (group <- config.acceptorAddresses(groupIndex)) yield {
      for (address <- group)
        yield chan[Acceptor[Transport]](address, Acceptor.serializer)
    }

  // ProxyLeader channels.
  private val proxyLeaders: Seq[Chan[ProxyLeader[Transport]]] =
    for (address <- config.proxyLeaderAddresses)
      yield chan[ProxyLeader[Transport]](address, ProxyLeader.serializer)

  // The round system used within this leader group.
  private val roundSystem =
    new RoundSystem.ClassicRoundRobin(config.leaderAddresses(groupIndex).size)

  // A round system used to figure out which leader groups are in charge of
  // which slots. For example, if we have 5 leader groups and we're leader
  // group 1 and we'd like to know which slot to use after slot 20, we can call
  // slotSystem.nextClassicRound(1, 20).
  private val slotSystem =
    new RoundSystem.ClassicRoundRobin(config.numLeaderGroups)

  // If the leader is the active leader, then this is its round. If it is
  // inactive, then this is the largest active round it knows about.
  @JSExport
  protected var round: Round = roundSystem
    .nextClassicRound(leaderIndex = 0, round = -1)

  // The next available slot in the log. Note that nextSlot is incremented by
  // `config.numLeaderGroups` every command, not incremented by 1. Log entries
  // are partitioned round robin across all leader groups.
  //
  // Even though we have a next slot into the log, you'll note that we don't
  // even have a log! Because we've decoupled aggressively, leaders don't
  // actually need a log at all.
  @JSExport
  protected var nextSlot: Slot = groupIndex
  metrics.nextSlot.set(nextSlot)

  // The largest nextSlot of any leader. Leaders periodically send their
  // nextSlot values to one another to update their highWatermarks.
  @JSExport
  protected var highWatermark: Slot = nextSlot
  metrics.highWatermark.set(highWatermark)

  // Every slot less than chosenWatermark has been chosen. Replicas
  // periodically send their chosenWatermarks to the leaders.
  @JSExport
  protected var chosenWatermark: Slot = 0
  metrics.chosenWatermark.set(chosenWatermark)

  // Leader election address. This field exists for the javascript
  // visualizations.
  @JSExport
  protected val electionAddress: Transport#Address =
    config.leaderElectionAddresses(groupIndex)(index)

  // Leader election participant.
  @JSExport
  protected val election = new Participant[Transport](
    address = electionAddress,
    transport = transport,
    logger = logger,
    addresses = config.leaderElectionAddresses(groupIndex),
    initialLeaderIndex = 0,
    options = options.electionOptions
  )
  election.register((leaderIndex) => {
    leaderChange(leaderIndex == index, recoverSlot = -1)
  })

  // The number of commands processed since the last time this leader
  // broadcasted its nextSlot to the other leaders.
  private var numCommandsSinceHighWatermarkSend: Int = 0

  // The number of Phase2a messages since the last flush.
  private var numPhase2asSentSinceLastFlush: Int = 0

  // A Mencius leader sends messages to ProxyLeaders one at a time.
  // `currentProxyLeader` is the proxy leader that is currently being sent to.
  // We initialize the first currentProxyLeader randomly so that different
  // leaders have the opportunity to send to different proxy leaders. Note that
  // this value is only used if config.distributionScheme is Hash.
  private var currentProxyLeader: Int = rand.nextInt(proxyLeaders.size)
  logger.info(s"Leader currentProxyLeader is initially $currentProxyLeader.")

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    startPhase1(round, chosenWatermark, recoverSlot = -1)
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

  // Consider a MultiPaxos deployment with two acceptor groups. The first group
  // is responsible for slots 0, 2, 4, and so on, while the second group is
  // responsible for slots 1, 3, 5, and so on.
  //
  // In Mencius, things are a bit more complicated. Imagine we have two leader
  // groups and each leader group has two acceptor groups. The first leader
  // group is responsible for slots 0, 2, 4, and so on, while the second leader
  // group is responsible for slots 1, 3, 5, and so on. Now, the first leader
  // group's slots are again split among its acceptor groups. The first
  // acceptor group is responsible for slots 0, 4, 8, and so on, while the
  // second acceptor group is responsible for slots 2, 6, 10, and so on.
  //
  // acceptorGroupIndexBySlot returns the index of the acceptor group
  // responsible for the given slot. It's assumed that the slot is owned by
  // this leader group.
  //
  //   Leaders Acceptors Slots
  //   o o o   o o o     0, 4, 8, 12, ...
  //           o o o     2, 6, 10, 14, ...
  //   o o o   o o o     1, 5, 9, 13, ...
  //           o o o     3, 7, 11, 15, ...
  private def acceptorGroupIndexBySlot(slot: Slot): Int = {
    logger.check(slotSystem.leader(slot) == groupIndex)
    (slot / config.numLeaderGroups) % config.acceptorAddresses(groupIndex).size
  }

  private def getProxyLeader(): Chan[ProxyLeader[Transport]] = {
    config.distributionScheme match {
      case Hash      => proxyLeaders(currentProxyLeader)
      case Colocated => proxyLeaders(groupIndex)
    }
  }

  private def thriftyQuorum(
      acceptors: Seq[Chan[Acceptor[Transport]]]
  ): Seq[Chan[Acceptor[Transport]]] =
    rand.shuffle(acceptors).take(config.quorumSize)

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
    logger.checkEq(state, Phase2)

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
        case Colocated => groupIndex
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

    // Flush our phase 2as, if needed.
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
          case Colocated => proxyLeaders(groupIndex).flush()
        }
      }
      numPhase2asSentSinceLastFlush = 0
      currentProxyLeader += 1
      if (currentProxyLeader >= config.numProxyLeaders) {
        currentProxyLeader = 0
      }
    }

    // Update our slot.
    nextSlot += config.numLeaderGroups
    metrics.nextSlot.set(nextSlot)

    // Broadcast our nextSlot, if needed. Note that we make sure to broadcast
    // our slot after updating it, not before.
    numCommandsSinceHighWatermarkSend += 1
    if (numCommandsSinceHighWatermarkSend >= options.sendHighWatermarkEveryN) {
      // We want to make sure we send watermarks on a different channel than
      // the current proxy leader to avoid messing up the flushing.
      val watermarkProxyLeaderIndex = config.distributionScheme match {
        case Hash =>
          if (currentProxyLeader + 1 == config.proxyLeaderAddresses.size) {
            0
          } else {
            currentProxyLeader + 1
          }
        case Colocated =>
          groupIndex
      }
      proxyLeaders(watermarkProxyLeaderIndex).send(
        ProxyLeaderInbound()
          .withHighWatermark(HighWatermark(nextSlot = nextSlot))
      )
      metrics.highWatermarksSentTotal.inc()
      numCommandsSinceHighWatermarkSend = 0
    }
  }

  private def startPhase1(
      round: Round,
      chosenWatermark: Slot,
      recoverSlot: Slot
  ): Phase1 = {
    val phase1a = Phase1a(round = round, chosenWatermark = chosenWatermark)
    for (group <- acceptors) {
      thriftyQuorum(group).foreach(
        _.send(AcceptorInbound().withPhase1A(phase1a))
      )
    }
    Phase1(
      phase1bs = mutable.Buffer
        .fill(config.acceptorAddresses(groupIndex).size)(mutable.Map()),
      pendingClientRequestBatches = mutable.Buffer(),
      recoverSlot = recoverSlot,
      resendPhase1as = makeResendPhase1asTimer(phase1a)
    )
  }

  private def leaderChange(isNewLeader: Boolean, recoverSlot: Slot): Unit = {
    metrics.leaderChangesTotal.inc()

    (state, isNewLeader) match {
      case (Inactive, false) =>
      // Do nothing.
      case (phase1: Phase1, false) =>
        phase1.resendPhase1as.stop()
        state = Inactive
      case (Phase2, false) =>
        state = Inactive
      case (Inactive, true) =>
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round,
                            chosenWatermark = chosenWatermark,
                            recoverSlot = recoverSlot)
      case (phase1: Phase1, true) =>
        phase1.resendPhase1as.stop()
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round,
                            chosenWatermark = chosenWatermark,
                            recoverSlot = recoverSlot)
      case (Phase2, true) =>
        round = roundSystem
          .nextClassicRound(leaderIndex = index, round = round)
        state = startPhase1(round = round,
                            chosenWatermark = chosenWatermark,
                            recoverSlot = recoverSlot)
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
        case Request.HighWatermark(_)            => "HighWatermark"
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
        case Request.HighWatermark(r) =>
          handleHighWatermark(src, r)
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
      case Inactive | Phase2 =>
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
        if (phase1.phase1bs.exists(_.size < config.quorumSize)) {
          return
        }

        // Find the largest slot with a vote. The `maxSlot` is either this
        // value or `phase1.recoverSlot` if that's larger. The max slot should
        // be owned by this leader group (or -1).
        val maxSlot =
          scala.math.max(
            phase1.phase1bs
              .map(
                groupPhase1bs => groupPhase1bs.values.map(maxPhase1bSlot).max
              )
              .max,
            phase1.recoverSlot
          )
        logger.check(maxSlot == -1 || slotSystem.leader(maxSlot) == groupIndex)

        // In MultiPaxos, we iterate from chosenWatermark to maxSlot proposing
        // safe values to fill in the log. In Mencius, we do the same, but we
        // only handle values that this leader group owns.
        for (slot <- roundSystem.nextClassicRound(
               leaderIndex = groupIndex,
               round = chosenWatermark - 1
             ) to maxSlot by config.numLeaderGroups) {
          val group = phase1.phase1bs(acceptorGroupIndexBySlot(slot))
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
        // slot is the first available slot after `maxSlot`.
        nextSlot = slotSystem.nextClassicRound(groupIndex, maxSlot)
        metrics.nextSlot.set(nextSlot)

        // Update our state.
        phase1.resendPhase1as.stop()
        state = Phase2

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
        client.send(
          ClientInbound()
            .withNotLeaderClient(NotLeaderClient(leaderGroupIndex = groupIndex))
        )

      case phase1: Phase1 =>
        // We'll process the client request after we've finished phase 1.
        phase1.pendingClientRequestBatches += ClientRequestBatch(
          batch = CommandBatch(command = Seq(clientRequest.command))
        )

      case Phase2 =>
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
        val batcher = chan[Batcher[Transport]](src, Batcher.serializer)
        batcher.send(
          BatcherInbound().withNotLeaderBatcher(
            NotLeaderBatcher(leaderGroupIndex = groupIndex,
                             clientRequestBatch = clientRequestBatch)
          )
        )

      case phase1: Phase1 =>
        // We'll process the client request after we've finished phase 1.
        phase1.pendingClientRequestBatches += clientRequestBatch

      case Phase2 =>
        processClientRequestBatch(clientRequestBatch)
    }
  }

  private def handleHighWatermark(
      src: Transport#Address,
      highWatermarkMessage: HighWatermark
  ): Unit = {
    // Ignore stale high watermarks.
    highWatermark = Math.max(nextSlot, highWatermark)
    metrics.highWatermark.set(highWatermark)
    if (highWatermarkMessage.nextSlot <= highWatermark) {
      logger.debug(
        s"A leader received a HighWatermark message with watermark " +
          s"${highWatermarkMessage.nextSlot} but already has a larger or " +
          s"equal highWatermark $highWatermark. The HighWatermark is being " +
          s"ignored."
      )
      return
    }

    // Update our watermark.
    highWatermark = highWatermarkMessage.nextSlot
    metrics.highWatermark.set(highWatermark)

    state match {
      case _: Phase1 | Inactive =>
      // If we receive a HighWatermark message, but we're not the active
      // leader, we still update our highWatermark, but we don't issue a
      // NoopRange. Only the active leader can do that.
      case Phase2 =>
        // If our nextSlot isn't lagging the highWatermark by that much, then
        // we don't do anything special.
        if (highWatermark - nextSlot < options.sendNoopRangeIfLaggingBy) {
          return
        }

        // If our nextSlot is lagging the highWatermark by quite a bit, then we
        // send a range noop out to fill in the holes.
        getProxyLeader().send(
          ProxyLeaderInbound().withPhase2ANoopRange(
            Phase2aNoopRange(
              slotStartInclusive = nextSlot,
              slotEndExclusive =
                slotSystem.nextClassicRound(groupIndex, highWatermark),
              round = round
            )
          )
        )
        metrics.noopRangesSentTotal.inc()
        nextSlot = slotSystem.nextClassicRound(groupIndex, highWatermark)
        metrics.nextSlot.set(nextSlot)
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

      case _: Phase1 | Phase2 =>
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound()
            .withLeaderInfoReplyClient(
              LeaderInfoReplyClient(leaderGroupIndex = groupIndex,
                                    round = round)
            )
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

      case _: Phase1 | Phase2 =>
        val batcher = chan[Batcher[Transport]](src, Batcher.serializer)
        batcher.send(
          BatcherInbound()
            .withLeaderInfoReplyBatcher(
              LeaderInfoReplyBatcher(leaderGroupIndex = groupIndex,
                                     round = round)
            )
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
        leaderChange(isNewLeader = true, recoverSlot = -1)
      case Phase2 =>
        round =
          roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
        leaderChange(isNewLeader = true, recoverSlot = -1)
    }
  }

  private def handleChosenWatermark(
      src: Transport#Address,
      msg: ChosenWatermark
  ): Unit = {
    chosenWatermark = Math.max(chosenWatermark, msg.slot)
    metrics.chosenWatermark.set(chosenWatermark)
  }

  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    // In MultiPaxos, we don't actually need to use `recover.slot` at all. If a
    // slot needs to be recovered, larger slots have been chosen. These larger
    // slots will be discovered in phase 1, and the leader will make sure all
    // smaller slots (including `recovers.slot`) are chosen.
    //
    // In Mencius, this is not true. One leader group, say group A, might
    // choose a slot and cause the recovery of a lower slot owned by a
    // different leader group, say group B. Group B won't see the slots chosen
    // by group A since the two groups use completely disjoint sets of
    // acceptors.
    //
    // Thus, we have to explicitly pass `recover.slot` through phase 1 and make
    // sure that the slot actually gets recovered.
    state match {
      case Inactive =>
      // Do nothing. The active leader will recover.
      case _: Phase1 | Phase2 =>
        // Leader change to make sure the slot is chosen.
        leaderChange(isNewLeader = true, recoverSlot = recover.slot)
    }
  }
}
