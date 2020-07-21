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
    // A Server implements its log as a BufferMap. `logGrowSize` is the
    // `growSize` argument used to construct the BufferMap.
    logGrowSize: Int,
    // Mencius round-robin partitions the log among a set of servers. If one
    // server executes faster than the others, then there can be large gaps in
    // the log. To prevent this, every once in a while, a server broadcasts its
    // nextSlot to all other servers. If a server's nextSlot is significantly
    // lagging the global maximum nextSlot, then we issue a range of noops. The
    // maximum nextSlot among all server groups is called a high watermark.
    //
    // The server broadcasts its nextSlot (indirectly through the proxy
    // servers) every `sendHighWatermarkEveryN` commands.
    sendHighWatermarkEveryN: Int,
    // Whenever a server updates its high watermark, if its nextSlot lags by
    // `sendNoopRangeIfLaggingBy` or more, then it issues a range of noops to
    // fill in the missing slots. If the server is lagging but not by much,
    // then it doesn't send noops.
    sendNoopRangeIfLaggingBy: Int,
    resendPhase1asPeriod: java.time.Duration,
    // A server flushes all of its channels to the proxy servers after every
    // `flushPhase2asEveryN` Phase2a messages sent. For example, if
    // `flushPhase2asEveryN` is 1, then the server flushes after every send.
    flushPhase2asEveryN: Int,
    electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object ServerOptions {
  val default = ServerOptions(
    logGrowSize = 1000,
    sendHighWatermarkEveryN = 10000,
    sendNoopRangeIfLaggingBy = 10000,
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    flushPhase2asEveryN = 1,
    electionOptions = ElectionOptions.default,
    measureLatencies = true
  )
}

@JSExportAll
class ServerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("vanilla_mencius_server_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val nextSlot: Gauge = collectors.gauge
    .build()
    .name("vanilla_mencius_server_next_slot")
    .help("The server's nextSlot.")
    .register()

  val highWatermark: Gauge = collectors.gauge
    .build()
    .name("vanilla_mencius_server_high_watermark")
    .help("The server's highWatermark.")
    .register()

  val chosenWatermark: Gauge = collectors.gauge
    .build()
    .name("vanilla_mencius_server_chosen_watermark")
    .help("The server's chosenWatermark.")
    .register()

  val highWatermarksSentTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_high_watermarks_sent_total")
    .help("Total number of high watermarks sent.")
    .register()

  val noopRangesSentTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_noop_ranges_sent_total")
    .help("Total number of noop ranges sent as a result of lagging nextSlot.")
    .register()

  val serverChangesTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_server_changes_total")
    .help("Total number of server changes.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_resend_phase1as_total")
    .help("Total number of times the server resent phase 1a messages.")
    .register()
}

@JSExportAll
class Server[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ServerOptions = ServerOptions.default,
    metrics: ServerMetrics = new ServerMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ServerInbound
  override val serializer = ServerInboundSerializer

  type ServerIndex = Int
  type Round = Int
  type Slot = Int

  @JSExportAll
  case class Phase1(
      startSlotInclusive: Slot,
      stopSlotExclusive: Slot,
      round: Round,
      phase1bs: mutable.Map[ServerIndex, Phase1b],
      resendPhase1as: Transport#Timer
  )

  @JSExportAll
  case class Phase2(
      round: Round,
      value: CommandOrNoop,
      phase2bs: mutable.Map[ServerIndex, Phase2b]
  )

  @JSExportAll
  sealed trait LogEntry

  @JSExportAll
  case class VotelessEntry(
      round: Round
  ) extends LogEntry

  @JSExportAll
  case class PendingEntry(
      round: Round,
      voteRound: Round,
      voteValue: CommandOrNoop
  ) extends LogEntry

  @JSExportAll
  case class ChosenEntry(
      value: CommandOrNoop
  ) extends LogEntry

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  @JSExport
  protected val index = config.serverAddresses.indexOf(address)

  // Server channels.
  private val servers: Seq[Chan[Server[Transport]]] =
    for (a <- config.serverAddresses)
      yield chan[Server[Transport]](a, Server.serializer)

  // Other server channels.
  private val otherServers: Seq[Chan[Server[Transport]]] =
    for (a <- config.serverAddresses if a != address)
      yield chan[Server[Transport]](a, Server.serializer)

  // Mencius rounds are a little complicated. Really, a server should own round
  // 0 for all of the slots that it owns, so every slot would have a different
  // round system. But for simplicity, we want to share a single recovery round
  // across the entire log. So, we use a single round robin round system, and
  // we make sure that all recover rounds start at n (for n servers). And then
  // we can just pretend that rounds 0 to n - 1 are all owned by the owner of
  // the slot. This is a little janky, but it's simple.
  private val roundSystem =
    new RoundSystem.ClassicRoundRobin(config.serverAddresses.size)

  // A round system used to figure out which server is in charge of which
  // slots. For example, if we have 5 servers and we're server
  // 1 and we'd like to know which slot to use after slot 20, we can call
  // slotSystem.nextClassicRound(1, 20).
  private val slotSystem =
    new RoundSystem.ClassicRoundRobin(config.serverAddresses.size)

  @JSExport
  val log = new frankenpaxos.util.BufferMap[LogEntry](options.logGrowSize)

  // The next available slot in the log. Note that nextSlot is incremented by
  // `config.serverAddresses.size` every command, not incremented by 1. Log
  // entries are partitioned round robin across all server groups.
  @JSExport
  protected var nextSlot: Slot = slotSystem.nextClassicRound(index, -1)

  // Consider a Mencius deployment with two servers. Server A is responsible
  // for slots 0, 2, 4, 6, and so on while server B is responsible for slots 1,
  // 3, 5, 7, and so on. Say server B has a nextSlot of 3 but receives a
  // Phase2a from server A in slot 10. Then, server B should skip slots 3, 5,
  // 7, and 9.
  //
  // skipSlots tracks which slots a server intends to skip. If skipSlots is
  // None, then no slots are skipped. In the example above, skipSlots would be
  // Some((3, 10)). Note that the first slot is inclusive and the second slot
  // is exclusive.
  //
  // Once a server has sent Skip messages to all the other nodes, then it can
  // clear skipSlots.
  @JSExport
  protected var skipSlots: Option[(Slot, Slot)] = None

  // When a server revokes slots from some other server, it uses this
  // monotonically increasing round to do so. See the comment above roundSystem
  // for why we start the recoverRound off so high.
  @JSExport
  protected var recoverRound: Round =
    roundSystem.nextClassicRound(index, config.serverAddresses.size - 1)

  // If a server thinks that some other server has failed, it attempts to
  // revoke some of its log entries. To do so, it first has to run Phase 1. To
  // keep things simple, we maintain the invariant that a server can only
  // perform one revocation for a given failed server at a time. We can revoke
  // two different failed servers at the same time, but not two revocations for
  // the _same_ failed server at the same time.
  @JSExport
  protected val phase1s = mutable.Map[ServerIndex, Phase1b]()

  @JSExport
  protected val phase2s = mutable.Map[Slot, Phase2]()

  // For every server, we keep track of the largest chosen slot owned by that
  // server. In the event that a server fails, we use revoke its slots starting
  // at its largest chosen slot.
  @JSExport
  protected val largestChosenSlots: mutable.Buffer[Int] =
    mutable.Buffer.fill(config.serverAddresses.size)(-1)

  // TODO(mwhittaker): Add heartbeat.
  // TODO(mwhittaker): Add revocation timers.

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
    // getProxyServer().send(
    //   ProxyServerInbound().withPhase2A(
    //     Phase2a(slot = nextSlot,
    //             round = round,
    //             commandBatchOrNoop = CommandBatchOrNoop()
    //               .withCommandBatch(clientRequestBatch.batch))
    //   )
    // )

    val proxyServerIndex = timed("processClientRequestBatch/getProxyServer") {
      config.distributionScheme match {
        case Hash      => currentProxyServer
        case Colocated => groupIndex
      }
    }

    val bytes = timed("processClientRequestBatch/serialize") {
      ProxyServerInbound()
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
        send(config.proxyServerAddresses(proxyServerIndex), bytes)
      }
      currentProxyServer += 1
      if (currentProxyServer >= config.numProxyServers) {
        currentProxyServer = 0
      }
    } else {
      timed("processClientRequestBatch/sendNoFlush") {
        sendNoFlush(config.proxyServerAddresses(proxyServerIndex), bytes)
      }
      numPhase2asSentSinceLastFlush += 1
    }

    if (numPhase2asSentSinceLastFlush >= options.flushPhase2asEveryN) {
      timed("processClientRequestBatch/flush") {
        config.distributionScheme match {
          case Hash      => proxyServers(currentProxyServer).flush()
          case Colocated => proxyServers(groupIndex).flush()
        }
      }
      numPhase2asSentSinceLastFlush = 0
      currentProxyServer += 1
      if (currentProxyServer >= config.numProxyServers) {
        currentProxyServer = 0
      }
    }

    // Update our slot.
    nextSlot += config.numServerGroups
    metrics.nextSlot.set(nextSlot)

    // Broadcast our nextSlot, if needed. Note that we make sure to broadcast
    // our slot after updating it, not before.
    numCommandsSinceHighWatermarkSend += 1
    if (numCommandsSinceHighWatermarkSend >= options.sendHighWatermarkEveryN) {
      // We want to make sure we send watermarks on a different channel than
      // the current proxy server to avoid messing up the flushing.
      val watermarkProxyServerIndex = config.distributionScheme match {
        case Hash =>
          if (currentProxyServer + 1 == config.proxyServerAddresses.size) {
            0
          } else {
            currentProxyServer + 1
          }
        case Colocated =>
          groupIndex
      }
      proxyServers(watermarkProxyServerIndex).send(
        ProxyServerInbound()
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

  private def serverChange(isNewServer: Boolean, recoverSlot: Slot): Unit = {
    metrics.serverChangesTotal.inc()

    (state, isNewServer) match {
      case (Inactive, false) =>
      // Do nothing.
      case (phase1: Phase1, false) =>
        phase1.resendPhase1as.stop()
        state = Inactive
      case (Phase2, false) =>
        state = Inactive
      case (Inactive, true) =>
        round = roundSystem
          .nextClassicRound(serverIndex = index, round = round)
        state = startPhase1(round = round,
                            chosenWatermark = chosenWatermark,
                            recoverSlot = recoverSlot)
      case (phase1: Phase1, true) =>
        phase1.resendPhase1as.stop()
        round = roundSystem
          .nextClassicRound(serverIndex = index, round = round)
        state = startPhase1(round = round,
                            chosenWatermark = chosenWatermark,
                            recoverSlot = recoverSlot)
      case (Phase2, true) =>
        round = roundSystem
          .nextClassicRound(serverIndex = index, round = round)
        state = startPhase1(round = round,
                            chosenWatermark = chosenWatermark,
                            recoverSlot = recoverSlot)
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ServerInbound.Request

    val label =
      inbound.request match {
        case Request.ClientRequest(_) => "ClientRequest"
        case Request.Phase1A(_)       => "Phase1a"
        case Request.Phase1B(_)       => "Phase1b"
        case Request.Phase2A(_)       => "Phase2a"
        case Request.Phase2B(_)       => "Phase2b"
        case Request.Skip(_)          => "Skip"
        case Request.Chosen(_)        => "Chosen"
        case Request.Nack(_)          => "Nack"
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r) => handleClientRequest(src, r)
        case Request.Phase1A(r)       => handlePhase1a(src, r)
        case Request.Phase1B(r)       => handlePhase1b(src, r)
        case Request.Phase2A(r)       => handlePhase2a(src, r)
        case Request.Phase2B(r)       => handlePhase2b(src, r)
        case Request.Skip(r)          => handleSkip(src, r)
        case Request.Chosen(r)        => handleChosen(src, r)
        case Request.Nack(r)          => handleNack(src, r)
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    // nextSlot should always point to a vacant slot in the log. If this is not
    // the case, something has gone wrong.
    logger.check(!states.contains(nextSlot))
    logger.check(!log.contains(nextSlot))

    // Vote for the command ourselves.
    val value = CommandOrNoop().withCommand(clientRequest.command)
    log.put(nextSlot, PendingEntry(voteRound = 0, voteValue = value))

    // If we have any pending slots to skip, send them to all the other servers
    // now. We'll flush them along with the Phase2a's in just a moment. This is
    // not the most extreme form of piggybacking possible. We could have
    // combined a Phase2a message and a Skip message into a single message to
    // avoid deserialiazing twice, but it should be good enough.
    skipSlots match {
      case None =>
        // Nothing to do.
        ()

      case Some((start, stop)) =>
        for (server <- otherServers) {
          server.sendNoFlush(
            ServerInbound().withSkip(
              Skip(
                serverIndex = index,
                startSlotInclusive = start,
                stopSlotExclusive = stop
              )
            )
          )
        }
    }

    // Send Phase2a's to all the other servers.
    for (server <- otherServers) {
      server.send(
        ServerInbound().withPhase2A(
          Phase2a(
            slot = nextSlot,
            round = 0,
            commandOrNoop = value
          )
        )
      )
    }

    // Update our state.
    states(nextSlot) = Phase2(
      round = 0,
      phase2bs = mutable.Map(
        index -> Phase2b(
          serverIndex = index,
          slot = nextSlot,
          round = 0
        )
      )
    )

    // Update our next slot.
    nextSlot = slotSystem.nextClassicRound(index, nextSlot)
  }

  private def handlePhase1a(
      src: Transport#Address,
      phase1a: Phase1a
  ): Unit = {
    val coordinator = chan[Server[Transport]](src, Server.serializer)
    val slotInfos = mutable.Buffer[Phase1bSlotInfo]()

    // Handling Phase1a's is a little bit awkward in Mencius. With MultiPaxos,
    // we have a single round for the entire log. That makes checking for stale
    // rounds easy. With Mencius, we don't have just one round for the entire
    // log. Nodes recover segments of the log at a time, meaning that some
    // segments can have higher rounds than others.
    //
    // Here, we handle this in probably not the optimal way, but in a simple
    // way. If the Phase1a is stale for _any_ of the slots, then we send back a
    // Nack. Otherwise, we send back the appropriate Phase2b's. Nacking should
    // hopefully be rare in practice, so the inefficiency shouldn't be a big
    // deal.
    var slot = phase1a.startSlotInclusive
    while (slot < phase1a.stopSlotExclusive) {
      log.get(slot) match {
        case None =>
          log.put(slot, VotelessEntry(phase1a.round))

        case Some(voteless: VotelessEntry) =>
          if (phase1a.round < voteless.round) {
            coordinator.send(
              ServerInbound().withNack(Nack(round = voteless.round))
            )
            return
          }
          log.put(slot, VotelessEntry(phase1a.round))

        case Some(pending: PendingEntry) =>
          if (phase1a.round < pending.round) {
            coordinator.send(
              ServerInbound().withNack(Nack(round = pending.round))
            )
            return
          }
          slotInfos += Phase1bSlotInfo(slot = slot).withPendingSlotInfo(
            PendingSlotInfo(voteRound = pending.voteRound,
                            voteValue = pending.voteValue)
          )
          log.put(slot, pending.copy(round = phase1a.round))

        case Some(chosen: ChosenEntry) =>
          slotInfos += Phase1bSlotInfo(slot = slot).withChosenSlotInfo(
            ChosenSlotInfo(value = chosen.value)
          )
      }
      slot = slotSystem.nextClassicRound(phase1a.serverIndex, slot)
    }

    coordinator.send(
      ServerInbound().withPhase1B(
        Phase1b(serverIndex = index,
                round = phase1a.round,
                info = slotInfos.toSeq)
      )
    )
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {

    // check to see if we're in phase 1 for it
    // check for stale rounds
    // wait for quorum
    // for every log entry, choose a safe value and propose something
    // update next slot???

    state match {
      case Inactive | Phase2 =>
        logger.debug(
          s"A server received a Phase1b message but is not in Phase1. Its " +
            s"state is $state. The Phase1b message is being ignored."
        )

      case phase1: Phase1 =>
        // Ignore messages from stale rounds.
        if (phase1b.round != round) {
          logger.debug(
            s"A server received a Phase1b message in round ${phase1b.round} " +
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
        // be owned by this server group (or -1).
        val maxSlot =
          scala.math.max(
            phase1.phase1bs
              .map(
                groupPhase1bs => groupPhase1bs.values.map(maxPhase1bSlot).max
              )
              .max,
            phase1.recoverSlot
          )
        logger.check(maxSlot == -1 || slotSystem.server(maxSlot) == groupIndex)

        // In MultiPaxos, we iterate from chosenWatermark to maxSlot proposing
        // safe values to fill in the log. In Mencius, we do the same, but we
        // only handle values that this server group owns.
        for (slot <- roundSystem.nextClassicRound(
               serverIndex = groupIndex,
               round = chosenWatermark - 1
             ) to maxSlot by config.numServerGroups) {
          val group = phase1.phase1bs(acceptorGroupIndexBySlot(slot))
          getProxyServer().send(
            ProxyServerInbound().withPhase2A(
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

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    // TODO(mwhittaker): Implement.
    val coordinator = chan[Server[Transport]](src, Server.serializer)
    log.get(phase2a.slot) match {
      case None                          =>
      case Some(voteless: VotelessEntry) =>
      case Some(pending: PendingEntry)   =>
      case Some(chosen: ChosenEntry)     =>
    }
    //
    // check log
    // nack if needed
    // vote for it
    // update skip slots very carefully
    // send back skips
    //
  }

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = {
    // TODO(mwhittaker): Implement.
  }

  private def handleSkip(
      src: Transport#Address,
      skip: Skip
  ): Unit = {
    // TODO(mwhittaker): Implement.
  }

  private def handleChosen(
      src: Transport#Address,
      chosen: Chosen
  ): Unit = {
    // TODO(mwhittaker): Implement.
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    // TODO(mwhittaker): Implement.
  }
}
