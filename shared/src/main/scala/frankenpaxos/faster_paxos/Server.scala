package frankenpaxos.fasterpaxos

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

// TODO(mwhittaker): Tidy up options.
@JSExportAll
case class ServerOptions(
    // When a delegate receives a Phase2a with a noop for a slot in which it
    // already has a command, it can send back a Phase2b with the command,
    // letting the sending delegate know about the command. If
    // ackNoopsWithCommands, servers do this. Otherwise, they ignore the
    // Phase2a.
    ackNoopsWithCommands: Boolean,
    // A Server implements its log as a BufferMap. `logGrowSize` is the
    // `growSize` argument used to construct the BufferMap.
    logGrowSize: Int,
    heartbeatOptions: HeartbeatOptions,
    resendPhase2aAnysPeriod: java.time.Duration,
    // A server flushes all of its channels to the proxy servers after every
    // `flushPhase2asEveryN` Phase2a messages sent. For example, if
    // `flushPhase2asEveryN` is 1, then the server flushes after every send.
    // flushPhase2asEveryN: Int,
    // electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object ServerOptions {
  val default = ServerOptions(
    ackNoopsWithCommands = true,
    heartbeatOptions = HeartbeatOptions.default,
    logGrowSize = 1000,
    resendPhase2aAnysPeriod = java.time.Duration.ofSeconds(5),
    // resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    // flushPhase2asEveryN = 1,
    // electionOptions = ElectionOptions.default,
    measureLatencies = true
  )
}

@JSExportAll
class ServerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("fasterpaxos_server_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  // val serverChangesTotal: Counter = collectors.counter
  //   .build()
  //   .name("fasterpaxos_server_server_changes_total")
  //   .help("Total number of server changes.")
  //   .register()
  //
  // val resendPhase1asTotal: Counter = collectors.counter
  //   .build()
  //   .name("fasterpaxos_server_resend_phase1as_total")
  //   .help("Total number of times the server resent phase 1a messages.")
  //   .register()

  val veryStaleClientRequestsTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_very_stale_client_requests_total")
    .help(
      "The total number of times a server received a ClientRequest so stale, " +
        "it didn't have a cached response for it in its client table."
    )
    .register()

  val staleClientRequestsTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_stale_client_requests_total")
    .help(
      "The total number of times a server received a stale ClientRequest and " +
        "did have a cached response for it in its client table."
    )
    .register()

  val tooFreshClientRequestRoundTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_too_fresh_client_request_round_total")
    .help(
      "The total number of times a server received a ClientRequest with a " +
        "round that was larger than its own (too fresh)."
    )
    .register()

  val pendingClientRequestsTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_pending_client_requests_total")
    .help(
      "The total number of times a server received a ClientRequest while in " +
        "Phase 1. These ClientRequest are stored as pending until the server " +
        "enters Phase 2."
    )
    .register()

  val sameRoundDelegatePhase1asTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_same_round_delegate_phase1as_total")
    .help(
      "Total number of Phase1as received by a Delegate in the same round. " +
        "A Delegate only becomes a Delegate after the leader finishes Phase " +
        "1 and proceeds ot Phase 2. Thus, a Delegate that receives a Phase1a " +
        "is a stale Phase1a from when it was still Idle."
    )
    .register()

  val chosenInPhase1Total: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_chosen_in_phase1_total")
    .help(
      "The total number of times a leader in Phase 1 learns of a chosen value."
    )
    .register()

  val executedUniqueCommandsTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_executed_unique_commands_total")
    .help(
      "The total number of unique commands executed. If a command is chosen " +
        "more than once, it only counts once towards this total."
    )
    .register()

  val executedDuplicatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_executed_duplicate_commands_total")
    .help(
      "The total number of duplicate commands \"executed\". A command can " +
        "be chosen more than once. Every time it is chosen, except the " +
        "first time, counts towards this total."
    )
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_executed_noops_total")
    .help("The total number of noops \"executed\".")
    .register()

  val resendPhase2aAnysTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_resend_phase2a_anys_total")
    .help("Total number of times the server resent Phase2aAny messages.")
    .register()

  val futurePhase2asTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_future_phase2as_total")
    .help(
      "The total number of times a server received a Phase2a with a " +
        "round that was larger than its own."
    )
    .register()

  val votedCommandGotNoopTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_voted_command_got_noop_total")
    .help(
      "The total number of times a server received a noop in a round and " +
        "log entry for which the server had already received a command."
    )
    .register()

  val staleRoundTotal: Counter = collectors.counter
    .build()
    .name("fasterpaxos_server_stale_round_total")
    .labelNames("type")
    .help("Total number of messages received with a stale round.")
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
  logger.check(config.serverAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ServerInbound
  override val serializer = ServerInboundSerializer

  // A Config contains an ordered list of server addresses. A server's server
  // index, is its index into this ordered list. For example, with servers
  // [0.0.0.0, 1.1.1.1, 2.2.2.2], the server with IP address 1.1.1.1 has server
  // index 0.
  //
  // When a leader begins executing a round, it selects a set of delegates. It
  // represents the delegates as an ordered list of server indexes. For
  // example, the delegates may be [4, 2, 6] consisting of servers 4, 2, and 6.
  // If a server is a delegate, its position in the list of delegates is its
  // DelegateIndex. For example server 4 has delegate index 0. It's important we
  // order the delgates and assign them indexes so that they know which slots
  // they own.
  //
  // Overall, server indexes and delegate indexes are very important, but also
  // very confusing and easy to get mixed up. We make each its own class to
  // hopefully avoid using the wrong kind of index.
  case class ServerIndex(x: Int)
  case class DelegateIndex(x: Int)

  type Round = Int
  type Slot = Int
  type ClientId = Int
  type ClientPseudonym = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case class Phase1(
      round: Round,
      delegates: Seq[ServerIndex],
      phase1bs: mutable.Map[ServerIndex, Phase1b],
      pendingClientRequests: mutable.Buffer[ClientRequest],
      resendPhase1as: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase2(
      round: Round,
      delegates: Seq[ServerIndex],
      delegateIndex: DelegateIndex,
      anyWatermark: Slot,
      var nextSlot: Slot,
      pendingValues: mutable.Map[Slot, CommandOrNoop],
      phase2bs: mutable.Map[Slot, mutable.Map[ServerIndex, Phase2b]],
      waitingPhase2aAnyAcks: mutable.Set[ServerIndex],
      resendPhase2aAnys: Transport#Timer
  ) extends State

  @JSExportAll
  case class Delegate(
      round: Round,
      delegates: Seq[ServerIndex],
      delegateIndex: DelegateIndex,
      anyWatermark: Slot,
      var nextSlot: Slot,
      pendingValues: mutable.Map[Slot, CommandOrNoop],
      phase2bs: mutable.Map[Slot, mutable.Map[ServerIndex, Phase2b]]
  ) extends State

  @JSExportAll
  case class Idle(
      round: Round,
      delegates: Seq[ServerIndex]
  ) extends State

  @JSExportAll
  sealed trait LogEntry

  @JSExportAll
  case class PendingEntry(
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

  private val index = ServerIndex(config.serverAddresses.indexOf(address))

  private val servers: Seq[Chan[Server[Transport]]] =
    for (a <- config.serverAddresses)
      yield chan[Server[Transport]](a, Server.serializer)

  private val otherServers: Seq[Chan[Server[Transport]]] =
    for (a <- config.serverAddresses if a != address)
      yield chan[Server[Transport]](a, Server.serializer)

  // `roundSystem` determines how rounds are partioned among the servers. For
  // now, we assume a simple round robin partitioning scheme. Server 0 owns
  // round 0, server 1 owns round 1, and so on.
  //
  // TODO(mwhittaker): Pass this in as a flag.
  private val roundSystem =
    new RoundSystem.ClassicRoundRobin(config.serverAddresses.size)

  // Within a round, delegates partition the slots between themselves. The
  // slotSystem determines this partitioning. For now, we assume a simple round
  // robin partitioning. For example, assume we have three delegates A, B, and
  // C. A owns slots 0, 3, 6, ...; B owns slots 1, 4, 7, ...; and C owns slots
  // 2, 5, 8, .... Note the difference between slotSystem and roundSystem. We
  // use f+1 here because that's how many delegates there are.
  //
  // TODO(mwhittaker): Pass this in as a flag.
  private val slotSystem =
    new RoundSystem.ClassicRoundRobin(config.f + 1)

  // Every log entry less than `executedWatermark` has been executed. There may
  // be commands larger than `executedWatermark` pending execution.
  // `executedWatermark` is public for testing.
  @JSExport
  var executedWatermark: Int = 0

  @JSExport
  val log = new frankenpaxos.util.BufferMap[LogEntry](options.logGrowSize)

  //  // Server election address. This field exists for the javascript
  //  // visualizations.
  //  @JSExport
  //  protected val electionAddress = config.serverElectionAddresses(index)
  //
  //  // Server election participant.
  //  @JSExport
  //  protected val election = new Participant[Transport](
  //    address = electionAddress,
  //    transport = transport,
  //    logger = logger,
  //    addresses = config.serverElectionAddresses,
  //    initialServerIndex = 0,
  //    options = options.electionOptions
  //  )
  //  election.register((serverIndex) => {
  //    serverChange(serverIndex == index)
  //  })

  // The server's state.
  // @JSExport
  // protected var state: State = if (index == 0) {
  //   startPhase1(round, chosenWatermark)
  // } else {
  //   Inactive
  // }

  // Leaders monitor acceptors to make sure they are still alive.
  @JSExport
  protected val heartbeatAddress: Transport#Address =
    config.heartbeatAddresses(index.x)

  @JSExport
  protected val heartbeat: frankenpaxos.heartbeat.Participant[Transport] =
    new frankenpaxos.heartbeat.Participant[Transport](
      address = heartbeatAddress,
      transport = transport,
      logger = logger,
      addresses = config.heartbeatAddresses,
      options = options.heartbeatOptions
    )

  // TODO(mwhittaker): Need to run a timer which checks for dead guys
  // if dead, random timer wait to become new leader

  @JSExport
  protected var state: State = ???

  // The client table used to ensure exactly once execution semantics. Every
  // entry in the client table is keyed by a clients address and its pseudonym
  // and maps to the largest executed id for the client and the result of
  // executing the command. Note that unlike with generalized protocols like
  // BPaxos and EPaxos, we don't need to use the more complex ClientTable
  // class. A simple map suffices.
  @JSExport
  protected var clientTable =
    mutable.Map[(ByteString, ClientPseudonym), (ClientId, ByteString)]()

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase2aAnysTimer(
      delegates: Seq[ServerIndex],
      phase2aAny: Phase2aAny
  ): Transport#Timer = {
    // TODO(mwhittaker): We only need to re-send the Phase2Anys to the
    // delegates which have not acked us yet.
    lazy val t: Transport#Timer = timer(
      s"resendPhase2aAnys",
      options.resendPhase2aAnysPeriod,
      () => {
        metrics.resendPhase2aAnysTotal.inc()
        for (serverIndex <- delegates if serverIndex != index) {
          servers(serverIndex.x)
            .send(ServerInbound().withPhase2AAny(phase2aAny))
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

  private def roundInfo(state: State): (Round, Seq[ServerIndex]) = {
    state match {
      case s: Phase1   => (s.round, s.delegates)
      case s: Phase2   => (s.round, s.delegates)
      case s: Delegate => (s.round, s.delegates)
      case s: Idle     => (s.round, s.delegates)
    }
  }

  private def stopTimers(state: State): Unit = {
    state match {
      case phase1: Phase1 => phase1.resendPhase1as.stop()
      case phase2: Phase2 => phase2.resendPhase2aAnys.stop()
      case _: Delegate    =>
      case _: Idle        =>
    }
  }

  // processClientRequest is called by a leader in Phase2 or by a delegate to
  // process a client request. processClientRequest returns the next open slot
  // to use.
  private def processClientRequest(
      delegates: Seq[ServerIndex],
      delegateIndex: DelegateIndex,
      slot: Slot,
      round: Round,
      clientRequest: ClientRequest,
      pendingValues: mutable.Map[Slot, CommandOrNoop],
      phase2bs: mutable.Map[Slot, mutable.Map[ServerIndex, Phase2b]]
  ): Slot = {
    // TODO(mwhittaker): Propose noops in the previous entries! We'll need to
    // pass anyWatermark to know which once are safe and which ones are not.
    ???

    log.get(slot) match {
      case Some(entry) =>
        logger.fatal(
          s"Server received a ClientRequest and went to process it in " +
            s"slot ${slot}, but the log already contains an entry in this " +
            s"slot: $entry. This is a bug."
        )

      case None =>
        // Send Phase2as to the other delegates.
        val commandOrNoop =
          CommandOrNoop().withCommand(clientRequest.command)
        for (serverIndex <- delegates if serverIndex != index) {
          servers(serverIndex.x).send(
            ServerInbound().withPhase2A(
              Phase2a(slot = slot, round = round, commandOrNoop = commandOrNoop)
            )
          )
        }

        // Vote for the value ourselves.
        logger.check(!phase2bs.contains(slot))
        log.put(slot,
                PendingEntry(voteRound = round, voteValue = commandOrNoop))
        phase2bs(slot) = mutable.Map(
          index -> Phase2b(serverIndex = index.x, slot = slot, round = round)
        )

        // Return our next slot.
        return slotSystem.nextClassicRound(
          leaderIndex = delegateIndex.x,
          round = slot
        )
    }
  }

  // Given the Phase1bSlotInfos returned in Phase1 for a given slot, compute a
  // safe value: a value v such that no value other than v has been or will be
  // chosen in any previous round.
  sealed trait SafeValue
  case class Safe(value: CommandOrNoop) extends SafeValue
  case class AlreadyChosen(value: CommandOrNoop) extends SafeValue

  private def safeValue(infos: Seq[Phase1bSlotInfo]): SafeValue = {
    // If there were no votes for this slot, then it is safe for us to
    // propose anything. We propose noop.
    if (infos.size == 0) {
      return Safe(CommandOrNoop().withNoop(Noop()))
    }

    // Segment the infos into pending and chosen info.
    val pendingSlotInfos = infos.flatMap(_.info.pendingSlotInfo)
    val chosenSlotInfos = infos.flatMap(_.info.chosenSlotInfo)

    // If a value has already been chosen, then we don't have to find a safe
    // value to propose. We don't have to propose anything, actually.
    chosenSlotInfos.headOption match {
      case Some(chosenSlotInfo) => return AlreadyChosen(chosenSlotInfo.value)
      case None                 =>
    }

    // At this point, infos is non-empty and full of votes. We find the largest
    // round in which a vote is cast and then focus on the votes in that round.
    // In normal Paxos, there can be only one value in this round. Here, there
    // can be a noop or a command. If there is only one value (a noop or a
    // command), we have to go with it. If there are both a noop and a command,
    // we go with the command.
    val largestRound = pendingSlotInfos.map(_.voteRound).max
    for (info <- pendingSlotInfos.filter(_.voteRound == largestRound)) {
      if (info.voteValue.value.isCommand) {
        return Safe(info.voteValue)
      }
    }
    Safe(CommandOrNoop().withNoop(Noop()))
  }

  private def pick(
      servers: Seq[Chan[Server[Transport]]],
      n: Int
  ): Seq[Chan[Server[Transport]]] =
    scala.util.Random.shuffle(servers).take(n)

  // `executeCommand(slot, command, clientReplies)` attempts to execute the
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
        case None | Some(_: PendingEntry) =>
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

  // processPhase2b captures the logic that a Leader or Delegate performs to
  // process a phase2b.
  private def processPhase2b(
      phase2b: Phase2b,
      delegateIndex: DelegateIndex,
      pendingValues: mutable.Map[Slot, CommandOrNoop],
      phase2bs: mutable.Map[Slot, mutable.Map[ServerIndex, Phase2b]]
  ): Unit = {
    // If this log entry has already been chosen, then we can skip the entire
    // protocol.
    log.get(phase2b.slot) match {
      case None | Some(_: PendingEntry) =>
      case Some(_: ChosenEntry)         => return
    }

    phase2b.command match {
      case None =>
        phase2bs(phase2b.slot)(ServerIndex(phase2b.serverIndex)) = phase2b

      case Some(command) =>
        // Okay, the situation here is a little complicated. Previously, we
        // sent a noop to the other delegates. A delegate received our noop
        // but had already received a value. They sent us back a Phase2b
        // not for the noop, but for the value. We want to ditch our noop
        // then, and go with the value. Also note that this may be the
        // first such valued Phase2b we received or a subsequent one.

        // Update our pendingValues and phase2bs.
        pendingValues(phase2b.slot).value match {
          // This is the first time receiving a valued nack.
          case CommandOrNoop.Value.Noop(Noop()) =>
            pendingValues(phase2b.slot) = CommandOrNoop().withCommand(command)
            phase2bs(phase2b.slot) = mutable.Map(
              // The other delegates Phase2b.
              ServerIndex(phase2b.serverIndex) -> phase2b,
              // Our Phase2b.
              index -> Phase2b(
                serverIndex = index.x,
                slot = phase2b.slot,
                round = phase2b.round
              )
            )

          // This is not the first time.
          case CommandOrNoop.Value.Command(existingCommand) =>
            logger.checkEq(command, existingCommand)
            phase2bs(phase2b.slot)(
              ServerIndex(phase2b.serverIndex)
            ) = phase2b

          case CommandOrNoop.Value.Empty =>
            logger.fatal("Empty CommandOrNoop encountered.")
        }

        // Wait for a quorum of Phase2bs.
        if (phase2bs(phase2b.slot).size < config.f + 1) {
          return
        }

        // Update our metadata.
        val chosen = pendingValues(phase2b.slot)
        log.put(phase2b.slot, ChosenEntry(chosen))
        pendingValues.remove(phase2b.slot)
        phase2bs.remove(phase2b.slot)

        // Send Phase3as to the other servers.
        otherServers.foreach(
          _.send(
            ServerInbound().withPhase3A(
              Phase3a(slot = phase2b.slot, commandOrNoop = chosen)
            )
          )
        )
    }

    executeLog((slot) => slotSystem.leader(slot) == delegateIndex.x)
  }

  // `maxPhase1bSlot(phase1b)` finds the largest slot present in `phase1b` or
  // -1 if no slots are present.
  // private def maxPhase1bSlot(phase1b: Phase1b): Slot = {
  //   if (phase1b.info.isEmpty) {
  //     -1
  //   } else {
  //     phase1b.info.map(_.slot).max
  //   }
  // }

  //  // Given a quorum of Phase1b messages, `safeValue` finds a value that is safe
  //  // to propose in a particular slot. If the Phase1b messages have at least one
  //  // vote in the slot, then the value with the highest vote round is safe.
  //  // Otherwise, everything is safe. In this case, we return Noop.
  //  private def safeValue(
  //      phase1bs: Iterable[Phase1b],
  //      slot: Slot
  //  ): CommandBatchOrNoop = {
  //    val slotInfos =
  //      phase1bs.flatMap(phase1b => phase1b.info.find(_.slot == slot))
  //    if (slotInfos.isEmpty) {
  //      CommandBatchOrNoop().withNoop(Noop())
  //    } else {
  //      slotInfos.maxBy(_.voteRound).voteValue
  //    }
  //  }
  //
  //  private def processClientRequestBatch(
  //      clientRequestBatch: ClientRequestBatch
  //  ): Unit = {
  //    logger.checkEq(state, Phase2)
  //
  //    // Normally, we'd have the following code, but to measure the time taken
  //    // for serialization vs sending, we split it up. It's less readable, but it
  //    // leads to some better performance insights.
  //    //
  //    // getProxyServer().send(
  //    //   ProxyServerInbound().withPhase2A(
  //    //     Phase2a(slot = nextSlot,
  //    //             round = round,
  //    //             commandBatchOrNoop = CommandBatchOrNoop()
  //    //               .withCommandBatch(clientRequestBatch.batch))
  //    //   )
  //    // )
  //
  //    val proxyServerIndex = timed("processClientRequestBatch/getProxyServer") {
  //      config.distributionScheme match {
  //        case Hash      => currentProxyServer
  //        case Colocated => index
  //      }
  //    }
  //
  //    val bytes = timed("processClientRequestBatch/serialize") {
  //      ProxyServerInbound()
  //        .withPhase2A(
  //          Phase2a(slot = nextSlot,
  //                  round = round,
  //                  commandBatchOrNoop = CommandBatchOrNoop()
  //                    .withCommandBatch(clientRequestBatch.batch))
  //        )
  //        .toByteArray
  //    }
  //
  //    if (options.flushPhase2asEveryN == 1) {
  //      // If we flush every message, don't bother managing
  //      // `numPhase2asSentSinceLastFlush` or flushing channels.
  //      timed("processClientRequestBatch/send") {
  //        send(config.proxyServerAddresses(proxyServerIndex), bytes)
  //      }
  //      currentProxyServer += 1
  //      if (currentProxyServer >= config.numProxyServers) {
  //        currentProxyServer = 0
  //      }
  //    } else {
  //      timed("processClientRequestBatch/sendNoFlush") {
  //        sendNoFlush(config.proxyServerAddresses(proxyServerIndex), bytes)
  //      }
  //      numPhase2asSentSinceLastFlush += 1
  //    }
  //
  //    if (numPhase2asSentSinceLastFlush >= options.flushPhase2asEveryN) {
  //      timed("processClientRequestBatch/flush") {
  //        config.distributionScheme match {
  //          case Hash      => proxyServers(currentProxyServer).flush()
  //          case Colocated => proxyServers(index).flush()
  //        }
  //      }
  //      numPhase2asSentSinceLastFlush = 0
  //      currentProxyServer += 1
  //      if (currentProxyServer >= config.numProxyServers) {
  //        currentProxyServer = 0
  //      }
  //    }
  //
  //    nextSlot += 1
  //  }
  //
  //  private def startPhase1(round: Round, chosenWatermark: Slot): Phase1 = {
  //    val phase1a = Phase1a(round = round, chosenWatermark = chosenWatermark)
  //    for (group <- acceptors) {
  //      thriftyQuorum(group).foreach(
  //        _.send(AcceptorInbound().withPhase1A(phase1a))
  //      )
  //    }
  //    Phase1(
  //      phase1bs = mutable.Buffer.fill(config.numAcceptorGroups)(mutable.Map()),
  //      pendingClientRequestBatches = mutable.Buffer(),
  //      resendPhase1as = makeResendPhase1asTimer(phase1a)
  //    )
  //  }
  //
  //  private def serverChange(isNewServer: Boolean): Unit = {
  //    metrics.serverChangesTotal.inc()
  //
  //    (state, isNewServer) match {
  //      case (Inactive, false) =>
  //      // Do nothing.
  //      case (phase1: Phase1, false) =>
  //        phase1.resendPhase1as.stop()
  //        state = Inactive
  //      case (Phase2, false) =>
  //        state = Inactive
  //      case (Inactive, true) =>
  //        round = roundSystem
  //          .nextClassicRound(serverIndex = index, round = round)
  //        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
  //      case (phase1: Phase1, true) =>
  //        phase1.resendPhase1as.stop()
  //        round = roundSystem
  //          .nextClassicRound(serverIndex = index, round = round)
  //        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
  //      case (Phase2, true) =>
  //        round = roundSystem
  //          .nextClassicRound(serverIndex = index, round = round)
  //        state = startPhase1(round = round, chosenWatermark = chosenWatermark)
  //    }
  //  }

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
        case Request.Phase2AAny(_)    => "Phase2aAny"
        case Request.Phase2AAnyAck(r) => "Phase2aAnyAck"
        case Request.Phase3A(_)       => "Phase3a"
        case Request.Recover(_)       => "Recover"
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
        case Request.Phase2AAny(r)    => handlePhase2aAny(src, r)
        case Request.Phase2AAnyAck(r) => handlePhase2aAnyAck(src, r)
        case Request.Phase3A(r)       => handlePhase3a(src, r)
        case Request.Recover(r)       => handleRecover(src, r)
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
    // Check the client table. If this ClientRequest is stale, we either ignore
    // it or send back our cached response (if we have one).
    val commandId = clientRequest.command.commandId
    val clientIdentity = (commandId.clientAddress, commandId.clientPseudonym)
    clientTable.get(clientIdentity) match {
      case None =>
        // The client request is not stale.
        {}

      case Some((largestClientId, cachedResult)) =>
        if (commandId.clientId < largestClientId) {
          logger.debug(
            s"Server received a stale ClientRequest, a ClientRequest so " +
              s"stale that we don't have a cached result. The ClientRequest " +
              s"is being ignored."
          )
          metrics.veryStaleClientRequestsTotal.inc()
          return
        } else if (commandId.clientId == largestClientId) {
          logger.debug(
            s"Server received a stale ClientRequest, but we have a cached " +
              s"response, so we're sending it back."
          )
          val client = chan[Client[Transport]](src, Client.serializer)
          client.send(
            ClientInbound().withClientReply(
              ClientReply(commandId = commandId, result = cachedResult)
            )
          )
          metrics.staleClientRequestsTotal.inc()
          return
        } else {
          // The client request is not stale.
        }
    }

    // Handle stale rounds and rounds from the future.
    val (round, delegates) = roundInfo(state)
    if (clientRequest.round < round) {
      // TODO(mwhittaker): If we're a leader or delegate, we could send back
      // this message but still process the request as normal.
      logger.debug(
        s"Server recevied a ClientRequest in round ${clientRequest.round} " +
          s"but is already in round $round. A RoundInfo is being sent to the " +
          s"client."
      )
      val client = chan[Client[Transport]](src, Client.serializer)
      client.send(
        ClientInbound()
          .withRoundInfo(
            RoundInfo(round = round, delegate = delegates.map(_.x))
          )
      )
      metrics.staleRoundTotal.labels("ClientRequest").inc()
      return
    } else if (clientRequest.round > round) {
      // TODO(mwhittaker): This server should maybe seek out the leader to
      // advance into the larger round. For now, we do nothing, as this case
      // should be rare, and doing nothing shouldn't violate liveness.
      logger.debug(
        s"Server recevied a ClientRequest in round ${clientRequest.round} " +
          s"but is only in round $round. The ClientRequest is being ignored."
      )
      metrics.tooFreshClientRequestRoundTotal.inc()
      return
    }

    state match {
      case phase1: Phase1 =>
        // We're in Phase 1, so we can't propose client commands just yet. We
        // buffer the command for now and later when we enter Phase 2, we
        // propose the command.
        phase1.pendingClientRequests += clientRequest
        metrics.pendingClientRequestsTotal.inc()

      case phase2: Phase2 =>
        phase2.nextSlot = processClientRequest(
          delegates = phase2.delegates,
          delegateIndex = phase2.delegateIndex,
          slot = phase2.nextSlot,
          round = round,
          clientRequest = clientRequest,
          pendingValues = phase2.pendingValues,
          phase2bs = phase2.phase2bs
        )

      case delegate: Delegate =>
        delegate.nextSlot = processClientRequest(
          delegates = delegate.delegates,
          delegateIndex = delegate.delegateIndex,
          slot = delegate.nextSlot,
          round = round,
          clientRequest = clientRequest,
          pendingValues = delegate.pendingValues,
          phase2bs = delegate.phase2bs
        )

      case idle: Idle =>
        logger.fatal(
          s"At this point, we've established that the client's round and our " +
            s"round are the same (round ${idle.round}). Yet, the client " +
            s"still sent to an idle server. This is a bug. Clients should " +
            s"only send to delegates."
        )
    }
  }

  private def handlePhase1a(src: Transport#Address, phase1a: Phase1a): Unit = {
    // Nack stale rounds.
    val (round, _) = roundInfo(state)
    if (phase1a.round < round) {
      logger.debug(
        s"Server recevied a Phase1a in round ${phase1a.round} but is already " +
          s"in round $round. A nack is being sent."
      )
      val leader = chan[Server[Transport]](src, Server.serializer)
      leader.send(ServerInbound().withNack(Nack(round = round)))
      metrics.staleRoundTotal.labels("Phase1a").inc()
      return
    }

    //            Stale Round   Same Round   Future Round
    //          +-------------+------------+----------------------+
    // Phase1   | ignore      | impossible | become idle; process |
    // Phase2   | ignore      | impossible | become idle; process |
    // Delegate | ignore      | ignore     | become idle; process |
    // Idle     | ignore      | process    | process              |
    //          +-------------+------------+----------------------+
    val idle = (state, phase1a.round == round) match {
      case (_: Delegate, true) =>
        // A Delegate only becomes a Delegate after the leader finishes Phase 1
        // and proceeds ot Phase 2. Thus, a Delegate that receives a Phase1a is
        // a stale Phase1a from when it was still Idle.
        logger.debug(
          s"Delegate received a Phase1a in round $round and is in round " +
            s"$round. The Phase1a is being ignored."
        )
        metrics.sameRoundDelegatePhase1asTotal.inc()
        return

      case (_: Phase1, true) | (_: Phase2, true) =>
        logger.fatal(
          s"Server in state $state received a Phase1a in its round " +
            s"(round $round), but this should be impossible."
        )

      case (idle: Idle, true) =>
        idle

      case (_, false) =>
        stopTimers(state)
        Idle(round = phase1a.round,
             delegates = phase1a.delegate.map(ServerIndex(_)))
    }
    state = idle

    val leader = chan[Server[Transport]](src, Server.serializer)
    val phase1b = Phase1b(
      serverIndex = index.x,
      round = idle.round,
      info = log
        .iteratorFrom(phase1a.chosenWatermark)
        .map({
          case (slot, pending: PendingEntry) =>
            Phase1bSlotInfo(slot = slot).withPendingSlotInfo(
              PendingSlotInfo(voteRound = pending.voteRound,
                              voteValue = pending.voteValue)
            )
          case (slot, chosen: ChosenEntry) =>
            Phase1bSlotInfo(slot = slot)
              .withChosenSlotInfo(ChosenSlotInfo(value = chosen.value))
        })
        .toSeq
    )
    leader.send(ServerInbound().withPhase1B(phase1b))
  }

  private def handlePhase1b(src: Transport#Address, phase1b: Phase1b): Unit = {
    // Ignore stale rounds.
    val (round, delegates) = roundInfo(state)
    if (phase1b.round < round) {
      logger.debug(
        s"Server recevied a Phase1b in round ${phase1b.round} but is already " +
          s"in round $round. The Phase1b is being ignored."
      )
      metrics.staleRoundTotal.labels("Phase1b").inc()
      return
    }

    state match {
      case _: Phase2 | _: Delegate | _: Idle =>
        // Note that unlike some other functions, we don't have to fuss with
        // checking to see if phase1b.round is larger than our round. That is
        // impossible. We can't receive Phase1bs in a round if we haven't send
        // Phase1as.
        logger.debug(
          s"Server received a Phase1b but is in state $state. The Phase1b " +
            s"is being ignored."
        )
        return

      case phase1: Phase1 =>
        // As noted above, we can't receive Phase1bs in a round in which we
        // didn't send Phase1as.
        logger.checkEq(phase1b.round, round)

        // Wait until we have a quorum of Phase1bs.
        phase1.phase1bs(ServerIndex(phase1b.serverIndex)) = phase1b
        if (phase1.phase1bs.size < config.f + 1) {
          return
        }

        // Find the largest slot with a vote.
        val slotInfos = phase1.phase1bs.values.flatMap(phase1b => phase1b.info)
        val maxSlot = if (slotInfos.size == 0) {
          -1
        } else {
          slotInfos.map(_.slot).max
        }

        // Iterate from executedWatermark to maxSlot proposing safe values to
        // fill in the log.
        val infosBySlot = slotInfos.groupBy(_.slot)
        val pendingValues = mutable.Map[Slot, CommandOrNoop]()
        val phase2bs = mutable.Map[Slot, mutable.Map[ServerIndex, Phase2b]]()
        for (slot <- executedWatermark to maxSlot) {
          safeValue(infosBySlot.get(slot).map(_.toSeq).getOrElse(Seq())) match {
            case AlreadyChosen(value) =>
              log.put(slot, ChosenEntry(value))
              metrics.chosenInPhase1Total.inc()

            case Safe(value) =>
              // Send Phase2as to a thrifty quorum of servers.
              for (server <- pick(otherServers, config.f)) {
                server.send(
                  ServerInbound().withPhase2A(
                    Phase2a(slot = slot, round = round, commandOrNoop = value)
                  )
                )
              }

              // Cast our own vote for the value.
              log.put(slot, PendingEntry(voteRound = round, voteValue = value))

              // Update our metadata.
              pendingValues(slot) = value
              phase2bs(slot) = mutable.Map(
                index -> Phase2b(serverIndex = index.x,
                                 slot = slot,
                                 round = round)
              )
          }
        }

        // We may have inserted some chosen commands into the log just now, so
        // we try and execute as much of the log as we can. We don't reply back
        // to the clients since they probably already received a reply.
        executeLog((slot) => false)

        for ((clientRequest, i) <- phase1.pendingClientRequests.zipWithIndex) {
          val slot = maxSlot + i + 1
          val value = CommandOrNoop().withCommand(clientRequest.command)

          // Send Phase2as to a thrifty quorum of servers.
          for (server <- pick(otherServers, config.f)) {
            server.send(
              ServerInbound().withPhase2A(
                Phase2a(slot = slot, round = round, commandOrNoop = value)
              )
            )
          }

          // Cast our own vote for the value.
          log.put(slot, PendingEntry(voteRound = round, voteValue = value))

          // Update our metadata.
          pendingValues(slot) = value
          phase2bs(slot) = mutable.Map(
            index -> Phase2b(serverIndex = index.x, slot = slot, round = round)
          )
        }

        // Send Phase2aAnys to delegates.
        val anyWatermark = maxSlot + phase1.pendingClientRequests.size + 1
        val phase2aAny = Phase2aAny(round = round,
                                    delegate = phase1.delegates.map(_.x),
                                    anyWatermark = anyWatermark)
        for (serverIndex <- phase1.delegates if serverIndex != index) {
          servers(serverIndex.x)
            .send(ServerInbound().withPhase2AAny(phase2aAny))
        }

        // If chosenWatermark = 3, maxSlot = 5, and we have two pending client
        // requests, then the log looks something like the picture below. We
        // have four different types of log entries:
        //
        // - A: The leader already knows these log entries have been chosen.
        // - B: The leader learned about these log entries during Phase 1 and
        //      gets them chosen with any quorum.
        // - C: The leader proposes pending client requests in these log entries.
        // - D: These log entries are empty. The delegates will get them full.
        //
        //     0   1   2   3   4   5   6   7   8   9
        //   +---+---+---+---+---+---+---+---+---+---+
        //   | A | A | A | B | B | B | C | C | D | D | ...
        //   +---+---+---+---+---+---+---+---+---+---+
        //               ^         ^         ^
        //               |         |         |
        //               chosenWatermark (3) |
        //                         |         |
        //                         maxSlot (5)
        //                                   |
        //                                   anyWatermark (7)

        val delegateIndex = DelegateIndex(phase1.delegates.indexOf(index))
        state = Phase2(
          round = round,
          delegates = phase1.delegates,
          delegateIndex = delegateIndex,
          anyWatermark = anyWatermark,
          nextSlot = slotSystem.nextClassicRound(leaderIndex = delegateIndex.x,
                                                 round = anyWatermark - 1),
          pendingValues = pendingValues,
          phase2bs = phase2bs,
          waitingPhase2aAnyAcks = phase1.delegates.to[mutable.Set],
          resendPhase2aAnys =
            makeResendPhase2aAnysTimer(phase1.delegates, phase2aAny)
        )
    }
  }

  private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
    // TODO(mwhittaker): Implement the f=1 optimization behind a flag.
    ???

    // Nack stale rounds.
    val (round, _) = roundInfo(state)
    if (phase2a.round < round) {
      logger.debug(
        s"Server recevied a Phase2a in round ${phase2a.round} but is already " +
          s"in round $round. A nack is being sent."
      )
      val leader = chan[Server[Transport]](src, Server.serializer)
      leader.send(ServerInbound().withNack(Nack(round = round)))
      metrics.staleRoundTotal.labels("Phase2a").inc()
      return
    }

    // Ignore rounds from the future. In regular Paxos, it's okay to receive a
    // Phase2a from a round larger than your own. Here, however, it's a little
    // more complicated. In theory, we could transition to being a delegate in
    // the future round, but we don't know the anyWatermark, the other
    // delegates, and so on. This makes things complicated. Instead, we just
    // ignore it and wait for the Phase2aAny.
    if (phase2a.round > round) {
      logger.debug(
        s"Server recevied a Phase2a in round ${phase2a.round} but is only " +
          s"in round $round. The Phase2a is being ignored."
      )
      metrics.futurePhase2asTotal.inc()
      return
    }

    state match {
      case _: Phase1 | _: Phase2 | _: Idle =>
        logger.fatal(
          s"Server in state $state received a Phase2a in its round " +
            s"(round $round), but this should be impossible."
        )

      case delegate: Delegate =>
        //                have noop   have cmd   have nothing
        //               +-----------+----------+------------+
        // received noop | vote noop | vote cmd | vote noop  |
        // received cmd  | vote cmd  | vote cmd | vote cmd   |
        //               +-----------+----------+------------+
        val sender = chan[Server[Transport]](src, Server.serializer)
        val pendingEntry =
          PendingEntry(voteRound = round, voteValue = phase2a.commandOrNoop)
        val phase2b = Phase2b(serverIndex = index.x,
                              slot = phase2a.slot,
                              round = round,
                              command = None)
        log.get(phase2a.slot) match {
          // If we already know of a chosen value, we can skip the entire
          // protocol and just let the sender delegate know.
          case Some(ChosenEntry(chosen)) =>
            sender.send(
              ServerInbound().withPhase3A(
                Phase3a(slot = phase2a.slot, commandOrNoop = chosen)
              )
            )

          // If we haven't voted for anything yet, then we vote for the value
          // the sender delegate sent to us.
          case None =>
            log.put(phase2a.slot, pendingEntry)
            sender.send(ServerInbound().withPhase2B(phase2b))

          case Some(pendingEntry: PendingEntry) =>
            import CommandOrNoop.Value
            (phase2a.commandOrNoop.value, pendingEntry.voteValue.value) match {
              // If we've already voted for a noop and receive a noop, then we
              // can safely re-send our Phase2a. This is just normal Paxos. If
              // we've already voted for a nnop and receive a command, it is
              // surprisingly safe for us to re-vote for the command. This is
              // special to Faster Paxos, but is in fact safe.
              case (_, Value.Noop(Noop())) =>
                log.put(phase2a.slot, pendingEntry)
                sender.send(ServerInbound().withPhase2B(phase2b))

              // If we've already voted for a command and received a command,
              // well, they must be the same command!
              case (Value.Command(proposed), Value.Command(voted)) =>
                logger.checkEq(proposed, voted)
                sender.send(ServerInbound().withPhase2B(phase2b))

              // See the documentation of ackNoopsWithCommands for information
              // on this optimization.
              case (Value.Noop(Noop()), Value.Command(command)) =>
                if (options.ackNoopsWithCommands) {
                  sender.send(
                    ServerInbound().withPhase2B(phase2b.withCommand(command))
                  )
                } else {
                  // Do nothing.
                }
                metrics.votedCommandGotNoopTotal.inc()

              case (Value.Empty, _) | (_, Value.Empty) =>
                logger.fatal("Empty CommandOrNoop encountered.")
            }
        }
    }
  }

  private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    // Ignore stale rounds.
    val (round, _) = roundInfo(state)
    if (phase2b.round < round) {
      logger.debug(
        s"Server recevied a Phase2b in round ${phase2b.round} but is already " +
          s"in round $round. The Phase2b is being ignored."
      )
      metrics.staleRoundTotal.labels("Phase2b").inc()
      return
    }

    // We can't receive a Phase2b in a round unless we've sent out Phase2as in
    // that round. Thus, we can receive Phase2bs from the future.
    logger.checkEq(phase2b.round, round)

    state match {
      case _: Phase1 | _: Idle =>
        logger.fatal(
          s"Server received a Phase2b in round $round but is in state " +
            s"$state. This should be impossible."
        )

      case phase2: Phase2 =>
        processPhase2b(phase2b,
                       phase2.delegateIndex,
                       phase2.pendingValues,
                       phase2.phase2bs)

      case delegate: Delegate =>
        processPhase2b(phase2b,
                       delegate.delegateIndex,
                       delegate.pendingValues,
                       delegate.phase2bs)
    }
  }

  private def handlePhase2aAny(
      src: Transport#Address,
      phase2aAny: Phase2aAny
  ): Unit = {
    // Ignore stale rounds.
    val (round, _) = roundInfo(state)
    if (phase2aAny.round < round) {
      logger.debug(
        s"Server recevied a Phase2aAny in round ${phase2aAny.round} but is " +
          s"already in round $round. The Phase2aAny is being ignored."
      )
      metrics.staleRoundTotal.labels("Phase2aAny").inc()
      return
    }

    state match {
      case phase1: Phase1 =>
        // We don't send Phase2aAnys to ourself.
        logger.checkGt(phase2aAny.round, round)

      case phase2: Phase2 =>
        // We don't send Phase2aAnys to ourself.
        logger.checkGt(phase2aAny.round, round)

      case delegate: Delegate =>
        if (phase2aAny.round == round) {
          logger.debug(
            s"Delegate received a Phase2aAny in round $round but is already " +
              s"in round $round. This must be a duplicate. We're sending " +
              s"back an ack."
          )
          val server = chan[Server[Transport]](src, Server.serializer)
          server.send(
            ServerInbound().withPhase2AAnyAck(
              Phase2aAnyAck(round = round, serverIndex = index.x)
            )
          )
          return
        }

      case idle: Idle =>
    }

    // Update our state.
    stopTimers(state)
    val delegateIndex = DelegateIndex(phase2aAny.delegate.indexOf(index.x))
    state = Delegate(
      round = phase2aAny.round,
      delegates = phase2aAny.delegate.map(ServerIndex(_)),
      delegateIndex = delegateIndex,
      anyWatermark = phase2aAny.anyWatermark,
      nextSlot =
        slotSystem.nextClassicRound(leaderIndex = delegateIndex.x,
                                    round = phase2aAny.anyWatermark - 1),
      pendingValues = mutable.Map[Slot, CommandOrNoop](),
      phase2bs = mutable.Map[Slot, mutable.Map[ServerIndex, Phase2b]]()
    )

    // Send back an ack.
    val server = chan[Server[Transport]](src, Server.serializer)
    server.send(
      ServerInbound().withPhase2AAnyAck(
        Phase2aAnyAck(round = phase2aAny.round, serverIndex = index.x)
      )
    )
  }

  private def handlePhase2aAnyAck(
      src: Transport#Address,
      phase2aAnyAck: Phase2aAnyAck
  ): Unit = {
    // Ignore stale rounds.
    val (round, _) = roundInfo(state)
    if (phase2aAnyAck.round < round) {
      logger.debug(
        s"Server recevied a Phase2aAnyAck in round ${phase2aAnyAck.round} " +
          s"but is already in round $round. The Phase2aAnyAck is being ignored."
      )
      metrics.staleRoundTotal.labels("Phase2aAnyAck").inc()
      return
    }

    // We can't receive a Phase2aAnyAck in a round unless we've sent out
    // Phase2aAnys in that round. Thus, we can receive Phase2aAnyAcks from the
    // future.
    logger.checkEq(phase2aAnyAck.round, round)

    state match {
      case _: Phase1 | _: Delegate | _: Idle =>
        logger.fatal(
          s"Server received a Phase2aAnyAck in round $round but is in " +
            s"state $state. This should be impossible."
        )

      case phase2: Phase2 =>
        phase2.waitingPhase2aAnyAcks.remove(
          ServerIndex(phase2aAnyAck.serverIndex)
        )
        if (phase2.waitingPhase2aAnyAcks.isEmpty) {
          phase2.resendPhase2aAnys.stop()
        }
    }
  }

  private def handlePhase3a(src: Transport#Address, phase3a: Phase3a): Unit = {
    log.put(phase3a.slot, ChosenEntry(phase3a.commandOrNoop))

    state match {
      case _: Phase1 | _: Idle =>
        // Do nothing.
        ()

      case phase2: Phase2 =>
        phase2.pendingValues.remove(phase3a.slot)
        phase2.phase2bs.remove(phase3a.slot)

      case delegate: Delegate =>
        delegate.pendingValues.remove(phase3a.slot)
        delegate.phase2bs.remove(phase3a.slot)
    }
  }

  private def handleRecover(src: Transport#Address, recover: Recover): Unit = {
    // If we've already chosen this log entry, then we're good!
    log.get(recover.slot) match {
      case None | Some(_: PendingEntry) =>
      case Some(chosen: ChosenEntry) =>
        val server = chan[Server[Transport]](src, Server.serializer)
        server.send(
          ServerInbound().withPhase3A(
            Phase3a(
              slot = recover.slot,
              commandOrNoop = chosen.value
            )
          )
        )
    }

    // Otherwise, we have to make sure the log enty gets chosen.
    state match {
      case phase1: Phase1 =>
      // Ignore. If nothing has failed, eventually the leader is going to
      // become a delegate and then weill handle the recover. If failure,
      // then we'll do a leader change.
      case phase2: Phase2 =>
      // Resend phase2as?
      case delegate: Delegate =>
      // Resend phase2as?
      case idle: Idle =>
      // Ignore. If nothing has failed, eventually the leader is going to
      // become a delegate and then weill handle the recover. If failure,
      // then we'll do a leader change.
    }
    ???
  }

  private def handleNack(src: Transport#Address, nack: Nack): Unit = {
    // TODO(mwhittaker): Implement.
    state match {
      case phase1: Phase1     =>
      case phase2: Phase2     =>
      case delegate: Delegate =>
      case idle: Idle         =>
    }
    ???
  }

  // TODO(mwhittaker): Add a recover timer.
}
