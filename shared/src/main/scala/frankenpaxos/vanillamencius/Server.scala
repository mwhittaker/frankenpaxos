package frankenpaxos.vanillamencius

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.election.basic.Participant
import frankenpaxos.heartbeat.HeartbeatOptions
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.StateMachine
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
    // Optimization 3 of the Mencius paper describes how a server revokes a
    // failed server's log entries. It is parameterized by beta.
    beta: Int,
    // Servers resend their Phase1a messages for liveness every so often. This
    // option determines the period with which they resend.
    resendPhase1asPeriod: java.time.Duration,
    // A server flushes Skips to the other servers every so often. This
    // option determines the period with which they flush.
    flushSkipSlotsPeriod: java.time.Duration,
    // If a server thinks another server is dead, it revokes some of its log
    // entries. This revocation happens perioidically, and the time between
    // revocations is chosen uniformly at random from the range defined by
    // these two options.
    revokeMinPeriod: java.time.Duration,
    revokeMaxPeriod: java.time.Duration,
    // A Server implements its log as a BufferMap. `logGrowSize` is the
    // `growSize` argument used to construct the BufferMap.
    logGrowSize: Int,
    // Servers use heartbeats to monitor which other servers are dead. The
    // heartbeat subprotocol uses these options.
    heartbeatOptions: HeartbeatOptions,
    measureLatencies: Boolean
)

@JSExportAll
object ServerOptions {
  val default = ServerOptions(
    beta = 1000,
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    flushSkipSlotsPeriod = java.time.Duration.ofSeconds(1),
    revokeMinPeriod = java.time.Duration.ofSeconds(1),
    revokeMaxPeriod = java.time.Duration.ofSeconds(5),
    logGrowSize = 1000,
    heartbeatOptions = HeartbeatOptions.default,
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

  val phase2bAlreadyChosenTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_phase2b_already_chosen_total")
    .help(
      "Total number of times a Server received a Phase2b for a slot that it " +
        "already knew was chosen."
    )
    .register()

  val phase2bMissingPhase2Total: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_phase2b_missing_phase2_total")
    .help(
      "Total number of times a Server received a Phase2b for a slot that it " +
        "did not have a Phase 2 for."
    )
    .register()

  val stalePhase2bsTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_stale_phase2bs_total")
    .help(
      "Total number of times a Server received a Phase2b in a stale round."
    )
    .register()

  val executedUniqueCommandsTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_executed_unique_commands_total")
    .help(
      "The total number of unique commands executed. If a command is chosen " +
        "more than once, it only counts once towards this total."
    )
    .register()

  val executedDuplicatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_executed_duplicate_commands_total")
    .help(
      "The total number of duplicate commands \"executed\". A command can " +
        "be chosen more than once. Every time it is chosen, except the " +
        "first time, counts towards this total."
    )
    .register()

  val revokeTimerTriggeredTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_revoke_timer_triggered_total")
    .help("Total number of times a revocation timer was triggered.")
    .register()

  val flushSkipSlotsTimerTriggeredTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_flush_skip_slots_timer_triggered_total")
    .help("Total number of times a flush skipSlots timer was triggered.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("vanilla_mencius_server_executed_noops_total")
    .help("The total number of noops \"executed\".")
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
    // Public for Javascript visualizations.
    val stateMachine: StateMachine,
    config: Config[Transport],
    options: ServerOptions = ServerOptions.default,
    metrics: ServerMetrics = new ServerMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ServerInbound
  override val serializer = ServerInboundSerializer

  type ClientId = Int
  type ClientPseudonym = Int
  type Round = Int
  type ServerIndex = Int
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

  // The client table used to ensure exactly once execution semantics. Every
  // entry in the client table is keyed by a clients address and its pseudonym
  // and maps to the largest executed id for the client and the result of
  // executing the command. Note that unlike with generalized protocols like
  // BPaxos and EPaxos, we don't need to use the more complex ClientTable
  // class. A simple map suffices.
  @JSExport
  protected var clientTable =
    mutable.Map[(ByteString, ClientPseudonym), (ClientId, ByteString)]()

  @JSExport
  val log = new frankenpaxos.util.BufferMap[LogEntry](options.logGrowSize)

  // Every log entry less than `executedWatermark` has been executed. There may
  // be commands larger than `executedWatermark` pending execution.
  // `executedWatermark` is public for testing.
  @JSExport
  var executedWatermark: Int = 0

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

  // A server has to flush its skipSlots every once in a while to ensure
  // liveness. With a lot of clients, this isn't a problem, but if we only have
  // one client, for example, the protocol can stall.
  @JSExport
  protected val flushSkipSlotsTimer = makeFlushSkipSlotsTimer()

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
  protected val phase1s = mutable.Map[ServerIndex, Phase1]()

  @JSExport
  protected val phase2s = mutable.Map[Slot, Phase2]()

  // For every server p, we keep track of the largest slot s such that s and
  // all previous slots owned by p are chosen. In the event that a server m
  // fails, we use revoke its slots starting after largestChosenPrefixSlots(m).
  @JSExport
  protected val largestChosenPrefixSlots: mutable.Buffer[Int] =
    mutable.Buffer.fill(config.serverAddresses.size)(-1)

  // Servers monitor other servers to make sure they are still alive.
  @JSExport
  protected val heartbeatAddress: Transport#Address =
    config.heartbeatAddresses(index)

  @JSExport
  protected val heartbeat: frankenpaxos.heartbeat.Participant[Transport] =
    new frankenpaxos.heartbeat.Participant[Transport](
      address = heartbeatAddress,
      transport = transport,
      logger = logger,
      addresses = config.heartbeatAddresses.filter(_ != heartbeatAddress),
      options = options.heartbeatOptions
    )

  @JSExport
  protected val revocationTimers = mutable.Map[ServerIndex, Transport#Timer]()
  for (i <- 0 until config.serverAddresses.size if i != index) {
    revocationTimers(i) = makeRevocationTimer(i)
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeFlushSkipSlotsTimer(): Transport#Timer = {
    timer(
      s"flushSkipSlotsTimer",
      options.flushSkipSlotsPeriod,
      () => {
        metrics.flushSkipSlotsTimerTriggeredTotal.inc()
        skipSlots match {
          case None =>
            logger.fatal(
              "A server's flushSkipSlotsTimer was triggered, but it doesn't " +
                "have any skipSlots to flush. This shouldn't be possible. " +
                "There must be a bug."
            )

          case Some((start, stop)) =>
            for (server <- otherServers) {
              server.send(
                ServerInbound().withSkip(
                  Skip(
                    serverIndex = index,
                    startSlotInclusive = start,
                    stopSlotExclusive = stop
                  )
                )
              )
            }
            skipSlots = None
        }
      }
    )
  }

  private def makeRevocationTimer(
      revokedServer: ServerIndex
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"revocationTimer $revokedServer",
      frankenpaxos.Util.randomDuration(options.revokeMinPeriod,
                                       options.revokeMaxPeriod),
      () => {
        metrics.revokeTimerTriggeredTotal.inc()

        if (heartbeat
              .unsafeAlive()
              .contains(config.heartbeatAddresses(revokedServer))) {
          // The server is alive. There's no need to revoke anything.
          t.start()
        } else if (largestChosenPrefixSlots(revokedServer) >=
                     nextSlot + options.beta) {
          // The server is dead but not lagging by enough for us to revoke its
          // log entries. See Optimization 3 in the Mencius paper for more
          // information on this.
          t.start()
        } else {
          // The server is dead and lagging. We need to revoke some of its log
          // entries. We send Phase1as to all of the servers, including
          // ourselves. Ideally, we would only send to the other servers and
          // process a Phase1a locally, but this is simpler and revocation is
          // rare, so performance is not paramount.
          val startSlotInclusive = largestChosenPrefixSlots(revokedServer)
          val stopSlotExclusive = nextSlot + (2 * options.beta)
          val phase1a = Phase1a(round = recoverRound,
                                startSlotInclusive = startSlotInclusive,
                                stopSlotExclusive = stopSlotExclusive)
          for (server <- servers) {
            server.send(ServerInbound().withPhase1A(phase1a))
          }

          // Update our state. Also note that we don't start the timer t
          // because we don't want to revoke during a revocation.
          phase1s(revokedServer) = Phase1(
            startSlotInclusive = startSlotInclusive,
            stopSlotExclusive = stopSlotExclusive,
            round = recoverRound,
            phase1bs = mutable.Map[ServerIndex, Phase1b](),
            resendPhase1as = makeResendPhase1asTimer(phase1a)
          )
          recoverRound = roundSystem.nextClassicRound(index, recoverRound)
        }
      }
    )
    t.start()
    t
  }

  private def makeResendPhase1asTimer(
      phase1a: Phase1a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as",
      options.resendPhase1asPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        for (server <- servers) {
          server.send(ServerInbound().withPhase1A(phase1a))
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

  def isChosen(slot: Slot): Boolean = {
    log.get(slot) match {
      case None | Some(_: VotelessEntry) | Some(_: PendingEntry) => false
      case Some(_: ChosenEntry)                                  => true
    }
  }

  // Propose a value (in Phase2a) in a given round and slot as part of a
  // revocation. Note that propose should not be used for a normal case
  // handling of a client request.
  def propose(round: Round, slot: Slot, value: CommandOrNoop): Unit = {
    // propose should only be used for revocation, and we shouldn't be revoking
    // ourselves.
    logger.checkNe(index, slotSystem.leader(slot))

    phase2s.get(slot) match {
      case Some(_: Phase2) =>
        // If we're already trying to recover this slot, then don't start the
        // recover process over. Let the existing Phase 2 run.
        ()

      case None =>
        // Vote for the value ourselves.
        log.get(slot) match {
          case Some(_: ChosenEntry) =>
            // A value has already been chosen in the log, so we don't have to
            // propose anythig.
            return

          case None =>
            log.put(
              slot,
              PendingEntry(round = round, voteRound = round, voteValue = value)
            )

          case Some(voteless: VotelessEntry) =>
            // Abort if our round is stale.
            if (round < voteless.round) {
              logger.debug(
                s"A server is trying to propose a value in slot $slot in " +
                  s"round $round, but this slot already has a vote in round " +
                  s"${voteless.round}. We are not proposing anything."
              )
              return
            }

            log.put(
              slot,
              PendingEntry(round = round, voteRound = round, voteValue = value)
            )

          case Some(pending: PendingEntry) =>
            // Abort if our round is stale.
            if (round < pending.round) {
              logger.debug(
                s"A server is trying to propose a value in slot $slot in " +
                  s"round $round, but this slot already has a vote in round " +
                  s"${pending.round}. We are not proposing anything."
              )
              return
            }

            log.put(
              slot,
              PendingEntry(round = round, voteRound = round, voteValue = value)
            )
        }

        // Send Phase2as to the other servers.
        for (server <- otherServers) {
          server.send(
            ServerInbound().withPhase2A(
              Phase2a(slot = slot, round = round, commandOrNoop = value)
            )
          )
        }

        // Update our state.
        phase2s(nextSlot) = Phase2(
          round = round,
          value = value,
          phase2bs = mutable.Map(
            index -> Phase2b(serverIndex = index, slot = slot, round = round)
          )
        )
    }
  }

  def choose(slot: Slot, value: CommandOrNoop): Unit = {
    // Update state.
    log.put(slot, ChosenEntry(value))
    phase2s.remove(slot)

    // Update our latest chosen prefix slot, if possible.
    val owningServer = slotSystem.leader(slot)
    if (owningServer != index) {
      var frontierSlot = slotSystem.nextClassicRound(
        owningServer,
        largestChosenPrefixSlots(owningServer)
      )
      while (isChosen(frontierSlot)) {
        largestChosenPrefixSlots(owningServer) = frontierSlot
        frontierSlot = slotSystem.nextClassicRound(owningServer, frontierSlot)
      }
    }

    // If this slot is owned by us and our nextSlot is smaller than it, then we
    // need to increase our nextSlot to be larger than it. This means that
    // there might temporarily be holes in the log, but those holes should be
    // filled in by the same machine that proposed the value in this slot.
    if (owningServer == index && nextSlot <= slot) {
      nextSlot = slotSystem.nextClassicRound(index, slot)
    }
  }

  // `executeCommand(slot, command, replyIf)` attempts to execute the
  // command `command` in slot `slot`. Attempting to execute `command` may or
  // may not produce a corresponding ClientReply. If the command is stale, it
  // may not produce a ClientReply. If it isn't stale, it will produce a
  // ClientReply.
  //
  // If a ClientReply is produced, it is sent back to the client, but only if
  // `replyIf` returns true.
  private def executeCommand(
      slot: Slot,
      command: Command,
      replyIf: (Slot => Boolean)
  ): Unit = {
    val commandId = command.commandId
    val clientIdentity = (commandId.clientAddress, commandId.clientPseudonym)
    val clientAddress = transport.addressSerializer
      .fromBytes(commandId.clientAddress.toByteArray())
    val client = chan[Client[Transport]](clientAddress, Client.serializer)

    clientTable.get(clientIdentity) match {
      case None =>
        val result =
          ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
        clientTable(clientIdentity) = (commandId.clientId, result)
        if (replyIf(slot)) {
          client.send(
            ClientInbound().withClientReply(
              ClientReply(commandId = commandId, result = result)
            )
          )
        }
        metrics.executedUniqueCommandsTotal.inc()

      case Some((largestClientId, cachedResult)) =>
        if (commandId.clientId < largestClientId) {
          metrics.executedDuplicatedCommandsTotal.inc()
        } else if (commandId.clientId == largestClientId) {
          // For liveness, we always send back the result here.
          client.send(
            ClientInbound().withClientReply(
              ClientReply(commandId = commandId, result = cachedResult)
            )
          )
          metrics.executedDuplicatedCommandsTotal.inc()
        } else {
          val result =
            ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
          clientTable(clientIdentity) = (commandId.clientId, result)
          if (replyIf(slot)) {
            client.send(
              ClientInbound().withClientReply(
                ClientReply(commandId = commandId, result = result)
              )
            )
          }
          metrics.executedUniqueCommandsTotal.inc()
        }
    }
  }

  private def executeLog(replyIf: (Slot) => Boolean): Unit = {
    while (true) {
      log.get(executedWatermark) match {
        case None | Some(_: VotelessEntry) | Some(_: PendingEntry) =>
          // There's a hole in the sky, through which things can fly. Er, I
          // mean, there's a hole in the log, so we can't execute anything.
          return

        case Some(ChosenEntry(value)) =>
          val slot = executedWatermark
          executedWatermark += 1

          value.value match {
            case CommandOrNoop.Value.Noop(Noop()) =>
              metrics.executedNoopsTotal.inc()
            case CommandOrNoop.Value.Command(command) =>
              executeCommand(slot, command, replyIf)
            case CommandOrNoop.Value.Empty =>
              logger.fatal("Empty CommandOrNoop encountered.")
          }
      }
    }

    logger.fatal(
      "The loop above should always return. This should be unreachable."
    )
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
        case Request.Phase1Nack(_)    => "Phase1Nack"
        case Request.Phase2Nack(_)    => "Phase2Nack"
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
        case Request.Phase1Nack(r)    => handlePhase1Nack(src, r)
        case Request.Phase2Nack(r)    => handlePhase2Nack(src, r)
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
    logger.check(!phase2s.contains(nextSlot))
    logger.check(!log.contains(nextSlot))

    // Vote for the command ourselves.
    val value = CommandOrNoop().withCommand(clientRequest.command)
    log.put(nextSlot, PendingEntry(round = 0, voteRound = 0, voteValue = value))

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
              Skip(serverIndex = index,
                   startSlotInclusive = start,
                   stopSlotExclusive = stop)
            )
          )
        }

        skipSlots = None
        flushSkipSlotsTimer.stop()
    }

    // Send Phase2a's to all the other servers.
    for (server <- otherServers) {
      server.send(
        ServerInbound().withPhase2A(
          Phase2a(slot = nextSlot, round = 0, commandOrNoop = value)
        )
      )
    }

    // Update our state.
    phase2s(nextSlot) = Phase2(
      round = 0,
      value = value,
      phase2bs = mutable.Map(
        index -> Phase2b(serverIndex = index, slot = nextSlot, round = 0)
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
    val revokedServerIndex = slotSystem.leader(phase1a.startSlotInclusive)
    while (slot < phase1a.stopSlotExclusive) {
      log.get(slot) match {
        case None =>
          log.put(slot, VotelessEntry(phase1a.round))

        case Some(voteless: VotelessEntry) =>
          if (phase1a.round < voteless.round) {
            coordinator.send(
              ServerInbound().withPhase1Nack(
                Phase1Nack(startSlotInclusive = phase1a.startSlotInclusive,
                           stopSlotExclusive = phase1a.stopSlotExclusive,
                           round = voteless.round)
              )
            )
            return
          }
          log.put(slot, VotelessEntry(phase1a.round))

        case Some(pending: PendingEntry) =>
          if (phase1a.round < pending.round) {
            coordinator.send(
              ServerInbound().withPhase1Nack(
                Phase1Nack(startSlotInclusive = phase1a.startSlotInclusive,
                           stopSlotExclusive = phase1a.stopSlotExclusive,
                           round = pending.round)
              )
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
      slot = slotSystem.nextClassicRound(revokedServerIndex, slot)
    }

    coordinator.send(
      ServerInbound().withPhase1B(
        Phase1b(serverIndex = index,
                round = phase1a.round,
                startSlotInclusive = phase1a.startSlotInclusive,
                stopSlotExclusive = phase1a.stopSlotExclusive,
                info = slotInfos.toSeq)
      )
    )
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    // Get our Phase1, or abort if there is none.
    val revokedServerIndex = slotSystem.leader(phase1b.startSlotInclusive)
    val phase1 = phase1s.get(revokedServerIndex) match {
      case None =>
        logger.debug(
          s"A server received a Phase1b for revoked server " +
            s"$revokedServerIndex, but doesn't have a corresponding " +
            s"Phase1. The Phase1b must be stale; it is being ignored."
        )
        return

      case Some(phase1: Phase1) =>
        phase1
    }

    // Ignore messages from stale rounds.
    if (phase1b.round != phase1.round) {
      logger.debug(
        s"A server received a Phase1b message in round ${phase1b.round} " +
          s"but is in round ${phase1.round}. The Phase1b is being ignored."
      )
      // If phase1b.round were larger than phase1.round, then we would have
      // received a nack instead of a Phase1b.
      logger.checkLt(phase1b.round, phase1.round)
      return
    }

    // Wait until we have a quorum of responses.
    phase1.phase1bs(phase1b.serverIndex) = phase1b
    if (phase1.phase1bs.size < config.f + 1) {
      return
    }

    // Fill in all the slots in the range (startSlotInclusive,
    // stopSlotExclusive].
    var slot = phase1.startSlotInclusive
    while (slot < phase1.stopSlotExclusive) {
      val slotInfos = phase1.phase1bs.values
        .flatMap(phase1b => phase1b.info.find(_.slot == slot))
      val pendingSlotInfos = slotInfos.flatMap(_.info.pendingSlotInfo)
      val chosenSlotInfos = slotInfos.flatMap(_.info.chosenSlotInfo)

      chosenSlotInfos.headOption match {
        case Some(info: ChosenSlotInfo) =>
          // A value was already chosen in this slot. We don't have to
          // propose anyting.
          choose(slot, info.value)

        case None =>
          if (pendingSlotInfos.isEmpty) {
            // No server has voted for any value in this slot, so we have to
            // propose a noop (remember that this is simple consensus).
            propose(round = phase1.round,
                    slot = slot,
                    value = CommandOrNoop().withNoop(Noop()))
          } else {
            // Some servers voted in this slot, so we propose the value v
            // associated with the largest voted round k.
            propose(round = phase1.round,
                    slot = slot,
                    value = pendingSlotInfos.maxBy(_.voteRound).voteValue)
          }

      }

      slot = slotSystem.nextClassicRound(revokedServerIndex, slot)
    }

    // We may have just chosen some command, so we try and execute the log as
    // best we can. We don't send back client replies though since we're
    // revoking.
    executeLog((_) => false)

    // End Phase 1.
    phase1.resendPhase1as.stop()
    phase1s.remove(revokedServerIndex)
    revocationTimers(revokedServerIndex).start()
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    // Ignore stale rounds.
    val coordinator = chan[Server[Transport]](src, Server.serializer)
    val round = log.get(phase2a.slot) match {
      case Some(chosen: ChosenEntry) =>
        coordinator.send(
          ServerInbound().withChosen(
            Chosen(slot = phase2a.slot, commandOrNoop = chosen.value)
          )
        )
        return

      case None =>
        -1
      case Some(voteless: VotelessEntry) =>
        voteless.round
      case Some(pending: PendingEntry) =>
        pending.round
    }

    if (phase2a.round < round) {
      coordinator.send(
        ServerInbound().withPhase2Nack(
          Phase2Nack(slot = phase2a.slot, round = round)
        )
      )
      return
    }

    // Update our state.
    log.put(phase2a.slot,
            PendingEntry(round = phase2a.round,
                         voteRound = phase2a.round,
                         voteValue = phase2a.commandOrNoop))

    // Update our nextSlot and skipSlots. Note that we send and clear our
    // skipSlots every time we process a client request. So, if our skipSlots
    // is not empty, we're guaranteed to have not proposed any values since we
    // set it. This means that we can extend the skipSlots range without
    // worrying about sending skips for slots in which we have already proposed
    // some other value.
    if (phase2a.slot > nextSlot) {
      val (start, stop) = skipSlots match {
        case None =>
          flushSkipSlotsTimer.start()
          (nextSlot, phase2a.slot)

        case Some((start, stop)) =>
          (start, phase2a.slot)
      }
      nextSlot = slotSystem.nextClassicRound(index, phase2a.slot)
      skipSlots = Some((start, stop))

      coordinator.sendNoFlush(
        ServerInbound().withSkip(
          Skip(serverIndex = index,
               startSlotInclusive = start,
               stopSlotExclusive = stop)
        )
      )
    }

    // Send the coordinator our vote.
    coordinator.send(
      ServerInbound().withPhase2B(
        Phase2b(serverIndex = index, slot = phase2a.slot, round = phase2a.round)
      )
    )
  }

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = {
    // If this command has already been chosen, then there's no need to run any
    // part of the protocol for it.
    log.get(phase2b.slot) match {
      case Some(_: ChosenEntry) =>
        metrics.phase2bAlreadyChosenTotal.inc()
        return

      case None | Some(_: VotelessEntry) | Some(_: PendingEntry) =>
        // Do nothing.
        ()
    }

    // Make sure we are actually running Phase 2 for this slot. If we're not
    // running Phase 2, then we ignore the Phase2b.
    val phase2 = phase2s.get(phase2b.slot) match {
      case None =>
        metrics.phase2bMissingPhase2Total.inc()
        return

      case Some(phase2: Phase2) =>
        phase2
    }

    // Check for staleness.
    if (phase2b.round < phase2.round) {
      metrics.stalePhase2bsTotal.inc()
      return
    }

    // We can only receive a Phase2b in a round if we sent a Phase2a in a
    // round, so we cannot receive Phase2bs from the future.
    logger.checkEq(phase2b.round, phase2.round)

    // Wait for a quorum of responses.
    phase2.phase2bs(phase2b.serverIndex) = phase2b
    if (phase2.phase2bs.size < config.f + 1) {
      return
    }

    // The value is chosen! Let the other servers know, and choose the value
    // myself. Now that the value is chosen, we can also start executing the
    // log.
    for (server <- otherServers) {
      server.send(
        ServerInbound()
          .withChosen(Chosen(slot = phase2b.slot, commandOrNoop = phase2.value))
      )
    }
    choose(phase2b.slot, phase2.value)
    executeLog((slot) => slotSystem.leader(slot) == index)
  }

  private def handleSkip(
      src: Transport#Address,
      skip: Skip
  ): Unit = {
    // Because we're implementing simple consensus, if we receive a skip, we
    // can always consider the value chosen. Why? Well, the leader is the only
    // person who can propose any value other than a noop, so if the leader has
    // committed to a noop, then we have no hope for any other value to ever be
    // chosen ever. Note that it is possible for a revoking server to propose a
    // non-noop value if it noticed that the leader had proposed a non-noop,
    // but because the leader has proposed a noop, that is impossible.
    var slot = skip.startSlotInclusive
    val coordinator = slotSystem.leader(skip.startSlotInclusive)
    while (slot < skip.stopSlotExclusive) {
      choose(slot, CommandOrNoop().withNoop(Noop()))
      slot = slotSystem.nextClassicRound(coordinator, slot)
    }
    executeLog((slot) => slotSystem.leader(slot) == index)
  }

  private def handleChosen(
      src: Transport#Address,
      chosen: Chosen
  ): Unit = {
    choose(chosen.slot, chosen.commandOrNoop)
    executeLog((slot) => slotSystem.leader(slot) == index)
  }

  private def handlePhase1Nack(
      src: Transport#Address,
      phase1Nack: Phase1Nack
  ): Unit = {
    // If we get Nacked, we should wait a little bit and try again. We should
    // also have some sort of leader election. Here, for simplicity, we just
    // ignore the nack.
    //
    // TODO(mwhittaker): Handle nacks more robustly if needed.
    return
  }

  private def handlePhase2Nack(
      src: Transport#Address,
      phase2Nack: Phase2Nack
  ): Unit = {
    // If we get Nacked, we should wait a little bit and try again. We should
    // also have some sort of leader election. Here, for simplicity, we just
    // ignore the nack.
    //
    // TODO(mwhittaker): Handle nacks more robustly if needed.
    return
  }
}
