package frankenpaxos.horizontal

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
import frankenpaxos.quorums.QuorumSystem
import frankenpaxos.quorums.QuorumSystemProto
import frankenpaxos.quorums.SimpleMajority
import frankenpaxos.quorums.UnanimousWrites
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.util
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
    // A Leader implements its log as a BufferMap. `logGrowSize` is the
    // `growSize` argument used to construct the BufferMap.
    logGrowSize: Int,
    // A configuration chosen at log index i takes effect in log entry i +
    // alpha.
    alpha: Int,
    // Leaders use timeouts to re-send requests and ensure liveness. This
    // duration determines how long a leader waits before re-sending Phase1as.
    resendPhase1asPeriod: java.time.Duration,
    electionOptions: ElectionOptions,
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    logGrowSize = 1000,
    alpha = 1000,
    resendPhase1asPeriod = java.time.Duration.ofSeconds(5),
    electionOptions = ElectionOptions.default,
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("horizontal_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val stalePhase1bsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_phase1bs_total")
    .help("Total number of stale Phase1bs received.")
    .register()

  val alphaOverflowTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_alpha_overflow_total")
    .help(
      "Total number of times an leader dropped a proposal because " +
        "there were more than alpha pending requests."
    )
    .register()

  val noChunksFoundTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_no_chunks_found_total")
    .help(
      "Total number of times an leader dropped a proposal because " +
        "it couldn't find a Phase 2 chunk with vacancies."
    )
    .register()

  val stalePhase2bsTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_phase2bs_total")
    .help("Total number of stale Phase2bs received.")
    .register()

  val phase2bAlreadyChosenTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_phase2b_already_chosen_total")
    .help(
      "Total number of Phase2bs received for a slot that was already chosen."
    )
    .register()

  val staleNackTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stale_acceptor_nack_total")
    .help("Total number of stale AcceptorNacks received.")
    .register()

  val ignoredRecoverTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_ignored_recover_total")
    .help("Total number of times an leader ignored a Recover.")
    .register()

  val activeLeaderReceivedChosenTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_active_leader_received_chosen_total")
    .help("Total number of times an active leader received a Chosen.")
    .register()

  val becomeLeaderTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_become_leader_total")
    .help("Total number of times a node becomes the leader.")
    .register()

  val stopBeingLeaderTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_stop_being_leader_total")
    .help("Total number of times a node stops being a leader.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("horizontal_leader_resend_phase1as_total")
    .help("Total number of times the leader resent Phase1a messages.")
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
  type ReplicaIndex = Int
  type Round = Int
  type Slot = Int

  @JSExportAll
  sealed trait Phase

  @JSExportAll
  case class Phase1(
      phase1bs: mutable.Map[AcceptorIndex, Phase1b],
      resendPhase1as: Transport#Timer
  ) extends Phase

  @JSExportAll
  case class Phase2(
      // The next free slot in the log, or None if the chunk is out of slots.
      var nextSlot: Option[Slot],
      // The values that the leader is trying to get chosen.
      values: mutable.Map[Slot, Value],
      // The Phase2b responses received from the acceptors.
      phase2bs: mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]
  ) extends Phase

  @JSExportAll
  case class Chunk(
      // The first and last slot owned by this chunk. The names are borrowed
      // from [1]. lastSlot is an Optional because a chunk does not always have
      // a last slot.
      //
      // [1]: scholar.google.com/scholar?cluster=5995547042370880548
      firstSlot: Slot,
      lastSlot: Option[Slot],
      // The configuration (a.k.a. quorum system) used in this chunk.
      quorumSystem: QuorumSystem[AcceptorIndex],
      // The state of the chunk.
      phase: Phase
  )

  @JSExportAll
  sealed trait State

  // One of the proposers is elected leader. All other proposers are inactive.
  @JSExportAll
  case class Inactive(
      // When this inactive proposer becomes a leader, it begins in the first
      // round it owns larger than `round`.
      round: Round
  ) extends State

  @JSExportAll
  case class Active(
      round: Round,
      // We divide the log into chunks, where each chunk is owned by a single
      // configuration. This similar to the approach taken in [1]. chunks
      // contains a list of all active chunks, sorted by firstSlot. We say a
      // chunk is _defunct_ if the leader knows that all of its slots are
      // chosen. A chunk that is not defunct is _active_.
      //
      // We may want to implement `chunks` as some sort of binary search tree
      // so that we can efficiently find the chunk that owns a slot, but in the
      // normal case, we have very few active chunks.
      //
      // [1]: scholar.google.com/scholar?cluster=5995547042370880548
      chunks: mutable.Buffer[Chunk]
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  private val index = config.leaderAddresses.indexOf(address)

  // Channels to all the _other_ leaders.
  private val otherLeaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses if a != address)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (a <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](a, Acceptor.serializer)

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // The log of commands. We implement the log as a BufferMap as opposed to
  // something like a SortedMap for efficiency.
  @JSExport
  protected val log = new util.BufferMap[Value](options.logGrowSize)

  // Every slot less than chosenWatermark has been chosen.
  @JSExport
  protected var chosenWatermark: Slot = 0

  // activeFirstSlots stores the firstSlots of all active chunks. This is
  // maintained by both active and inactive leaders. It is a little redundant
  // with the chunks stored by the active leader.
  @JSExport
  protected var activeFirstSlots = mutable.Buffer[Slot](0)

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
    if (leaderIndex == index) {
      logger.info("Becoming leader due to leader election!")
      becomeLeader(getNextRound(state))
    } else {
      stopBeingLeader()
    }
  })

  // The leader's state.
  @JSExport
  protected var state: State = if (index == 0) {
    // In the first round, we use a predetermined set of acceptors. This is not
    // necessary, but it is convenient for some benchmarks.
    val quorumSystem =
      new SimpleMajority((0 until (2 * config.f + 1)).toSet, seed)
    Active(
      round = 0,
      chunks = mutable.Buffer(
        makeChunk(round = 0, firstSlot = 0, quorumSystem = quorumSystem)
      )
    )
  } else {
    Inactive(round = -1)
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase1asTimer(
      quorumSystem: QuorumSystem[AcceptorIndex],
      phase1a: Phase1a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as",
      options.resendPhase1asPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        for (i <- quorumSystem.nodes()) {
          acceptors(i).send(AcceptorInbound().withPhase1A(phase1a))
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

  private def getRound(state: State): Round = {
    state match {
      case inactive: Inactive => inactive.round
      case active: Active     => active.round
    }
  }

  // Returns the Chunk that owns `slot`. If no such Chunk exists (i.e. if
  // `slot` is less than `chunks(0)`
  private def getChunk(
      chunks: mutable.Buffer[Chunk],
      slot: Slot
  ): Option[(Int, Chunk)] = {
    logger.check(!chunks.isEmpty)

    for ((chunk, i) <- chunks.zipWithIndex.reverseIterator) {
      if (slot >= chunk.firstSlot) {
        return Some((i, chunk))
      }
    }

    None
  }

  // Returns the smallest round owned by this leader that is larger than any
  // round in `state`.
  private def getNextRound(state: State): Round =
    roundSystem.nextClassicRound(leaderIndex = index, round = getRound(state))

  private def stopPhaseTimers(phase: Phase): Unit = {
    phase match {
      case phase1: Phase1 => phase1.resendPhase1as.stop()
      case phase2: Phase2 =>
    }
  }

  private def stopTimers(state: State): Unit = {
    state match {
      case _: Inactive => ()

      case active: Active =>
        for (chunk <- active.chunks) {
          stopPhaseTimers(chunk.phase)
        }
    }
  }

  private def choose(
      slot: Slot,
      value: Value
  ): mutable.Buffer[(Slot, Configuration)] = {
    log.put(slot, value)

    val configurations = mutable.Buffer[(Slot, Configuration)]()
    while (true) {
      log.get(chosenWatermark) match {
        case Some(value) =>
          val slot = chosenWatermark
          chosenWatermark += 1
          value.value match {
            case Value.Value.Command(_) | Value.Value.Noop(_) =>
              // Do nothing.
              {}

            case Value.Value.Configuration(configuration) =>
              activeFirstSlots += slot + options.alpha
              configurations.append((slot, configuration))

            case Value.Value.Empty =>
              logger.fatal("Empty Value encountered.")
          }

          // If we execute the first slot of the second chunk, then the first
          // chunk is now defunct.
          if (activeFirstSlots.size >= 2 && slot == activeFirstSlots(1)) {
            activeFirstSlots.remove(0)
          }

        case None =>
          // We "execute" the log in prefix order, so if we hit an empty slot,
          // we have to stop "executing".
          return configurations
      }
    }

    configurations
  }

  // `makeChunk` makes a new chunk in Phase 1.
  private def makeChunk(
      round: Round,
      firstSlot: Slot,
      quorumSystem: QuorumSystem[AcceptorIndex]
  ): Chunk = {
    // Send Phase1as to all of the acceptors.
    val phase1a = Phase1a(
      round = round,
      firstSlot = firstSlot,
      chosenWatermark = chosenWatermark
    )
    for (i <- quorumSystem.nodes()) {
      acceptors(i).send(AcceptorInbound().withPhase1A(phase1a))
    }

    Chunk(
      firstSlot = firstSlot,
      lastSlot = None,
      quorumSystem = quorumSystem,
      phase = Phase1(
        phase1bs = mutable.Map(),
        resendPhase1as = makeResendPhase1asTimer(quorumSystem, phase1a)
      )
    )
  }

  // Given a set of Phase1b messages, `safeValue` finds a value that is safe to
  // propose in a particular slot. If the Phase1b messages have at least one
  // vote in the slot, then the value with the highest vote round is safe.
  // Otherwise, everything is safe. In this case, we return Noop.
  private def safeValue(phase1bs: Iterable[Phase1b], slot: Slot): Value = {
    val slotInfos =
      phase1bs.flatMap(phase1b => phase1b.info.find(_.slot == slot))
    if (slotInfos.isEmpty) {
      Value().withNoop(Noop())
    } else {
      slotInfos.maxBy(_.voteRound).voteValue
    }
  }

  private def stopBeingLeader(): Unit = {
    metrics.stopBeingLeaderTotal.inc()
    stopTimers(state)
    state = Inactive(getRound(state))
  }

  private def becomeLeader(newRound: Round): Unit = {
    logger.debug(s"Becoming leader in round $newRound.")

    logger.checkGt(newRound, getRound(state))
    logger.check(roundSystem.leader(newRound) == index)
    metrics.becomeLeaderTotal.inc()

    stopTimers(state)
    if (activeFirstSlots(0) == 0) {
      state = Active(
        round = newRound,
        chunks = mutable.Buffer(
          makeChunk(
            newRound,
            firstSlot = 0,
            quorumSystem =
              new SimpleMajority((0 until (2 * config.f + 1)).toSet, seed)
          )
        )
      )
    } else {
      val quorumSystem = log.get(activeFirstSlots(0) - options.alpha) match {
        case None =>
          logger.fatal(
            s"activeFirstSlots(0) is $activeFirstSlots(0), yet there is no " +
              s"chunk in slot activeFirstSlots(0) - options.alpha " +
              s"(${activeFirstSlots(0) - options.alpha}). This should be " +
              s"impossible. It must be a bug."
          )

        case Some(value) =>
          value.value match {
            case Value.Value.Command(_) =>
              logger.fatal(
                s"activeFirstSlots(0) is $activeFirstSlots(0), yet the " +
                  s"entry in slot activeFirstSlots(0) - options.alpha " +
                  s"(${activeFirstSlots(0) - options.alpha}) is a command. " +
                  s"This should be impossible. It must be a bug."
              )

            case Value.Value.Noop(_) =>
              logger.fatal(
                s"activeFirstSlots(0) is $activeFirstSlots(0), yet the " +
                  s"entry in slot activeFirstSlots(0) - options.alpha " +
                  s"(${activeFirstSlots(0) - options.alpha}) is a noop. " +
                  s"This should be impossible. It must be a bug."
              )

            case Value.Value.Configuration(configuration) =>
              QuorumSystem.fromProto(configuration.quorumSystem)

            case Value.Value.Empty =>
              logger.fatal("Empty Value encountered.")
          }
      }

      state = Active(
        round = newRound,
        chunks = mutable.Buffer(
          makeChunk(
            newRound,
            firstSlot = activeFirstSlots(0),
            quorumSystem = quorumSystem
          )
        )
      )
    }
  }

  private def propose(active: Active, value: Value): Unit = {
    // We need to iterate over the chunks and find the first one that is in
    // Phase 2 with vacant slots. Note that a Phase 2 chunk may have already
    // filled all of its slots. In this case, we obviously cannot propose a
    // value in the chunk.
    //
    // If we can't find a chunk, then we drop the message.
    for (chunk <- active.chunks) {
      chunk.phase match {
        case phase1: Phase1 =>
          // Do nothing.
          {}

        case phase2: Phase2 =>
          phase2.nextSlot match {
            case None =>
              // This chunk is full, we do nothing and move on to the next
              // chunk.
              ()

            case Some(nextSlot) =>
              // We can only have alpha commands pending at any given point
              // in time. If this command would violate that, drop it.
              //
              // TODO(mwhittaker): Maybe we should buffer these commands
              // and send them out later once we've chosen more commands?
              if (nextSlot >= chosenWatermark + options.alpha) {
                metrics.alphaOverflowTotal.inc()
                return
              }

              // Send Phase2as to a write quorum.
              val phase2a = Phase2a(firstSlot = chunk.firstSlot,
                                    slot = nextSlot,
                                    round = active.round,
                                    value = value)
              for (index <- chunk.quorumSystem.randomWriteQuorum()) {
                acceptors(index).send(
                  AcceptorInbound().withPhase2A(phase2a)
                )
              }

              // Update our metadata.
              logger.check(!phase2.values.contains(nextSlot))
              phase2.values(nextSlot) = value
              phase2.phase2bs(nextSlot) = mutable.Map()
              phase2.nextSlot = chunk.lastSlot match {
                case None => Some(nextSlot + 1)
                case Some(lastSlot) =>
                  if (nextSlot == lastSlot) {
                    None
                  } else {
                    Some(nextSlot + 1)
                  }
              }
              return
          }
      }
    }

    metrics.noChunksFoundTotal.inc()
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import LeaderInbound.Request

    val label =
      inbound.request match {
        case Request.Phase1B(_)           => "Phase1b"
        case Request.ClientRequest(_)     => "ClientRequest"
        case Request.Phase2B(_)           => "Phase2b"
        case Request.Chosen(_)            => "Chosen"
        case Request.Reconfigure(_)       => "Reconfigure"
        case Request.LeaderInfoRequest(_) => "LeaderInfoRequest"
        case Request.Nack(_)              => "Nack"
        case Request.Recover(_)           => "Recover"
        case Request.Die(_)               => "Die"
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1B(r)           => handlePhase1b(src, r)
        case Request.ClientRequest(r)     => handleClientRequest(src, r)
        case Request.Phase2B(r)           => handlePhase2b(src, r)
        case Request.Chosen(r)            => handleChosen(src, r)
        case Request.Reconfigure(r)       => handleReconfigure(src, r)
        case Request.LeaderInfoRequest(r) => handleLeaderInfoRequest(src, r)
        case Request.Nack(r)              => handleNack(src, r)
        case Request.Recover(r)           => handleRecover(src, r)
        case Request.Die(r)               => handleDie(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    // Ignore messages from stale rounds.
    val round = getRound(state)
    if (phase1b.round != round) {
      logger.debug(
        s"A leader received a Phase1b message in round ${phase1b.round} " +
          s"but is in round $round. The Phase1b is being ignored."
      )
      // We can't receive phase 1bs from the future.
      logger.checkLt(phase1b.round, round)
      metrics.stalePhase1bsTotal.inc()
      return
    }

    state match {
      case _: Inactive =>
        logger.debug(
          s"A leader received a Phase1b but is inactive. The Phase1b is " +
            s"being ignored."
        )

      case active: Active =>
        getChunk(active.chunks, phase1b.firstSlot) match {
          case None =>
            logger.debug(
              s"Leader received a Phase1b message in slot " +
                s"${phase1b.firstSlot}, but has no matching chunk. The " +
                s"Phase1b message must be stale. It is being ignored."
            )

          case Some((chunkIndex, chunk)) =>
            chunk.phase match {
              case phase2: Phase2 =>
                logger.debug(
                  s"Leader received a Phase1b message but is in Phase2. The " +
                    s"Phase1b message is being ignored."
                )

              case phase1: Phase1 =>
                // Wait until we have a read quorum.
                phase1.phase1bs(phase1b.acceptorIndex) = phase1b
                if (!chunk.quorumSystem.isSuperSetOfReadQuorum(
                      phase1.phase1bs.keys.toSet
                    )) {
                  return
                }

                // Stop our timers.
                stopPhaseTimers(phase1)

                // Find the largest slot with a vote, or -1 if there are no
                // votes.
                val slotInfos = phase1.phase1bs.values.flatMap(_.info)
                val maxSlot = if (slotInfos.isEmpty) {
                  -1
                } else {
                  slotInfos.map(_.slot).max
                }

                // Now, we iterate from chosenWatermark to maxSlot proposing
                // safe values to the acceptors to fill in the log. Note that
                // we are not checking the alpha overflow condition here.
                //
                // TODO(mwhittaker): I believe that not checking the alpha
                // condition here is safe. We could only get a Phase1b in a
                // slot in this chunk if some other leader proposed it in a
                // regular Phase 2. It would only do so if it did not overrun
                // the alpha limit and therefore we can conclude that this slot
                // is ownned by the chunk. Double check this though.
                val values = mutable.Map[Slot, Value]()
                val phase2bs =
                  mutable.Map[Slot, mutable.Map[AcceptorIndex, Phase2b]]()
                for (slot <- Math.max(phase1b.firstSlot, chosenWatermark) to
                       maxSlot) {
                  val value = safeValue(phase1.phase1bs.values, slot)
                  val phase2a = Phase2a(firstSlot = chunk.firstSlot,
                                        slot = slot,
                                        round = active.round,
                                        value = value)
                  for (index <- chunk.quorumSystem.randomWriteQuorum()) {
                    acceptors(index).send(
                      AcceptorInbound().withPhase2A(phase2a)
                    )
                  }

                  values(slot) = value
                  phase2bs(slot) = mutable.Map()
                }

                val s = Seq(phase1b.firstSlot, chosenWatermark, maxSlot + 1).max
                val nextSlot = chunk.lastSlot match {
                  case None =>
                    Some(s)
                  case Some(lastSlot) =>
                    if (s > lastSlot) {
                      None
                    } else {
                      Some(s)
                    }
                }

                // Update our state.
                active.chunks(chunkIndex) = chunk.copy(
                  phase = Phase2(
                    nextSlot = nextSlot,
                    values = values,
                    phase2bs = phase2bs
                  )
                )
            }
        }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    state match {
      case _: Inactive =>
        // If we're not the active leader but receive a request from a client,
        // then we send back a NotLeader message to let them know that they've
        // contacted the wrong leader.
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(ClientInbound().withNotLeader(NotLeader()))

      case active: Active =>
        propose(active, Value().withCommand(clientRequest.command))
    }
  }

  private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    // Ignore messages from stale rounds.
    val round = getRound(state)
    if (phase2b.round != round) {
      logger.debug(
        s"A leader received a Phase2b message in round ${phase2b.round} " +
          s"but is in round $round. The Phase2b is being ignored."
      )
      metrics.stalePhase2bsTotal.inc()
      return
    }

    // Ignore messages for slots that have already been chosen.
    if (phase2b.slot < chosenWatermark || log.get(phase2b.slot).isDefined) {
      logger.debug(
        s"A leader received a Phase2b message in slot ${phase2b.slot} " +
          s"but that slot has already been chosen. The Phase2b message " +
          s"is being ignored."
      )
      metrics.phase2bAlreadyChosenTotal.inc()
      return
    }

    state match {
      case _: Inactive =>
        logger.debug(
          s"Leader received a Phase2b message but is Inactive. The Phase2b " +
            s"message is being ignored."
        )

      case active: Active =>
        getChunk(active.chunks, phase2b.slot) match {
          case None =>
            logger.debug(
              s"Leader received a Phase2b message in slot ${phase2b.slot}, " +
                s"but has no matching chunk. The Phase2b message must be " +
                s"stale. It is being ignored."
            )

          case Some((chunkIndex, chunk)) =>
            chunk.phase match {
              case phase1: Phase1 =>
                logger.debug(
                  s"Leader received a Phase2b message but is in Phase1. The " +
                    s"Phase2b message is being ignored."
                )

              case phase2: Phase2 =>
                // Wait until we have a write quorum.
                val phase2bs = phase2.phase2bs(phase2b.slot)
                phase2bs(phase2b.acceptorIndex) = phase2b
                if (!chunk.quorumSystem.isWriteQuorum(phase2bs.keys.toSet)) {
                  return
                }

                // Inform the replicas and other leaders that the value has
                // been chosen.
                val value = phase2.values(phase2b.slot)
                val chosen = Chosen(slot = phase2b.slot, value = value)
                replicas.foreach(_.send(ReplicaInbound().withChosen(chosen)))
                otherLeaders.foreach(_.send(LeaderInbound().withChosen(chosen)))

                // Update our metadata.
                phase2.values.remove(phase2b.slot)
                phase2.phase2bs.remove(phase2b.slot)

                // Add any new chunks.
                val configurations = choose(phase2b.slot, value)
                for ((slot, configuration) <- configurations) {
                  // Update the previous chunk's last slot.
                  val lastSlot = slot + options.alpha - 1
                  active.chunks(active.chunks.size - 1) = active.chunks.last
                    .copy(lastSlot = Some(lastSlot))

                  // If the previous chunk is in Phase 2, it is possible that
                  // its nextSlot is now equal to one past its last slot. If
                  // this is the case, we have to change its nextSlot to None.
                  active.chunks.last.phase match {
                    case _: Phase1 =>
                      // Nothing to do.
                      {}

                    case phase2: Phase2 =>
                      phase2.nextSlot match {
                        case None =>
                          logger.fatal(
                            "A chunk with no lastSlot has a None nextSlot. " +
                              "This should be impossible. There must be a bug."
                          )

                        case Some(nextSlot) =>
                          if (nextSlot > lastSlot) {
                            phase2.nextSlot = None
                          }
                      }
                  }

                  // Add the new chunk.
                  active.chunks.append(
                    makeChunk(active.round,
                              slot + options.alpha,
                              QuorumSystem
                                .fromProto(configuration.quorumSystem))
                  )
                }

                // Remove any defunct chunks.
                val chunks = active.chunks.dropWhile(chunk => {
                  chunk.lastSlot match {
                    case None           => false
                    case Some(lastSlot) => lastSlot < chosenWatermark
                  }
                })
                state = active.copy(chunks = chunks)
            }
        }
    }
  }

  private def handleChosen(src: Transport#Address, chosen: Chosen): Unit = {
    state match {
      case _: Inactive =>
        choose(chosen.slot, chosen.value)

      case _: Active =>
        // An active leader received a Chosen message. An active leader doesn't
        // send Chosen messages to istelf, so this message must be stale or be
        // from another concurrently active leader. Both cases are rare, so we
        // simply ignore the message. If we do decide to process the message,
        // things are more complicated.
        metrics.activeLeaderReceivedChosenTotal.inc()
    }
  }

  private def handleReconfigure(
      src: Transport#Address,
      reconfigure: Reconfigure
  ): Unit = {
    state match {
      case _: Inactive =>
        // Do nothing. The active leader will reconfigure.
        {}

      case active: Active =>
        propose(active, Value().withConfiguration(reconfigure.configuration))
    }
  }

  private def handleLeaderInfoRequest(
      src: Transport#Address,
      leaderInfoRequest: LeaderInfoRequest
  ): Unit = {
    state match {
      case _: Inactive =>
        // We're inactive, so we ignore the leader info request. The active
        // leader will respond to the request.
        {}

      case active: Active =>
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound().withLeaderInfoReply(
            LeaderInfoReply(round = active.round)
          )
        )
    }
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    val round = getRound(state)
    if (nack.round < round) {
      logger.debug(
        s"A Leader received a Nack message with round ${nack.round} but is " +
          s"already in round $round. The Nack is being ignored."
      )
      metrics.staleNackTotal.inc()
      return
    }

    state match {
      case inactive: Inactive =>
        state = inactive.copy(round = nack.round)

      case active: Active =>
        val newRound = roundSystem.nextClassicRound(
          leaderIndex = index,
          round = Math.max(nack.round, active.round)
        )
        becomeLeader(newRound)
    }
  }

  // TODO(mwhittaker): This recovery technique was taken from Matchmaker Paxos
  // and doesn't fully work for Horizontal MultiPaxos.
  private def handleRecover(
      src: Transport#Address,
      recover: Recover
  ): Unit = {
    state match {
      case _: Inactive =>
        // Do nothing. The active leader will recover.
        {}

      case active: Active =>
        // If our chosen watermark is larger than the slot we're trying to
        // recover, then we won't get the value chosen again. We have to lower
        // our watermark so that the protocol gets the value chosen again. This
        // works for Matchmaker MultiPaxos, but not here since the
        // chosenWatermark is more important to track. For example, our
        // activeFirstSlots(0) might be wrong if we lower the chosenWatermark.
        // We might also violate alpha. Instead, we just ignore the Recover.
        // The replica might recover from another replica. Also, this recovery
        // is very rare anyway.
        if (chosenWatermark > recover.slot) {
          metrics.ignoredRecoverTotal.inc()
          return
        }

        // Leader change to make sure the slot is chosen.
        becomeLeader(
          roundSystem.nextClassicRound(leaderIndex = index,
                                       round = active.round)
        )
    }
  }

  private def handleDie(src: Transport#Address, die: Die): Unit = {
    logger.fatal("Die!")
  }

  // API ///////////////////////////////////////////////////////////////////////
  // For the JS frontend.
  def reconfigure(): Unit = {
    state match {
      case _: Inactive =>
        // Do nothing. The active leader will reconfigure.
        {}

      case active: Active =>
        val quorumSystem = new SimpleMajority(
          rand
            .shuffle(List() ++ (0 until acceptors.size))
            .take(2 * config.f + 1)
            .toSet,
          rand.nextLong()
        )
        propose(active,
                Value().withConfiguration(
                  Configuration(QuorumSystem.toProto(quorumSystem))
                ))
    }
  }
}
