package frankenpaxos.fastmultipaxos

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import scala.scalajs.js.annotation._

@JSExportAll
object AcceptorInboundSerializer extends ProtoSerializer[AcceptorInbound] {
  type A = AcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Acceptor {
  val serializer = AcceptorInboundSerializer
}

@JSExportAll
class AcceptorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val batchesTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_acceptor_batches_total")
    .help("Total number of ProposeRequest batches processed.")
    .register()

  val proposeRequestsInBatchesTotal: Counter = collectors.counter
    .build()
    .name("fast_multipaxos_acceptor_propose_requests_in_batches_total")
    .help("Total number of ProposeRequests processed in a batch.")
    .register()
}

@JSExportAll
case class AcceptorOptions(
    // With Fast MultiPaxos, it's possible that two clients concurrently
    // propose two conflicting commands and that those commands arrive at
    // acceptors in different orders preventing either from being chosen. This
    // is called a conflict, and the performance of Fast MultiPaxos degrades
    // as the number of conflicts increases.
    //
    // As a heuristic to avoid conflicts, we have acceptors buffer messages and
    // process them in batches in a deterministic order. Every `waitPeriod`
    // seconds, an acceptor forms a batch of all propose requests that are
    // older than `waitStagger`, sorts them deterministically, and process
    // them.
    //
    // TODO(mwhittaker): I don't think waitStagger is actually useful. Verify
    // that it's pointless and remove it.
    // TODO(mwhittaker): Is there a smarter way to reduce the number of
    // conflicts?
    waitPeriod: java.time.Duration,
    waitStagger: java.time.Duration
)

@JSExportAll
object AcceptorOptions {
  val default = AcceptorOptions(
    waitPeriod = java.time.Duration.ofMillis(25),
    waitStagger = java.time.Duration.ofMillis(25)
  )
}

@JSExportAll
class Acceptor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: AcceptorOptions = AcceptorOptions.default,
    metrics: AcceptorMetrics = new AcceptorMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  override type InboundMessage = AcceptorInbound
  override val serializer = Acceptor.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the Paxos configuration and compute the acceptor's id.
  logger.check(config.acceptorAddresses.contains(address))
  private val acceptorId = config.acceptorAddresses.indexOf(address)

  // The largest round in which this acceptor has received a message. Note that
  // we perform the common MultiPaxos optimization in which every acceptor has
  // a single round for every slot. Also note that `config.roundSystem` can be
  // used to convert the round into a leader.
  @JSExport
  protected var round: Int = -1

  // Channels to the leaders.
  private val leaders: Map[Int, Chan[Leader[Transport]]] = {
    for ((leaderAddress, i) <- config.leaderAddresses.zipWithIndex)
      yield i -> chan[Leader[Transport]](leaderAddress, Leader.serializer)
  }.toMap

  // Every acceptor runs a heartbeat participant to inform the leaders that it
  // is still alive.
  @JSExport
  protected val heartbeatAddress: Transport#Address =
    config.acceptorHeartbeatAddresses(acceptorId)
  @JSExport
  protected val heartbeat: frankenpaxos.heartbeat.Participant[Transport] =
    new frankenpaxos.heartbeat.Participant[Transport](heartbeatAddress,
                                                      transport,
                                                      logger,
                                                      Set())

  // See the documentation for AcceptorOptions.waitPeriod and
  // AcceptorOptions.waitStagger above. Long is the value reported by
  // System.nanoTime. Using something like java.time.Instant does not work
  // because the granularity is not fine enough.
  @JSExport
  protected var bufferedProposeRequests =
    mutable.Buffer[(Long, Transport#Address, ProposeRequest)]()

  private val processBufferedProposeRequestsTimer: Option[Transport#Timer] =
    // If the wait period and wait stagger are both 0, then we don't bother
    // fiddling with a timer; we process propose requests immediately. Thus, we
    // don't even create a timer.
    if (options.waitPeriod == java.time.Duration.ofSeconds(0) &&
        options.waitStagger == java.time.Duration.ofSeconds(0)) {
      None
    } else {
      val t = timer(
        "processBufferedProposeRequests",
        options.waitPeriod,
        () => {
          processBufferedProposeRequests()
          processBufferedProposeRequestsTimer.get.start()
        }
      )
      t.start()
      Some(t)
    }

  // In Fast Paxos, every acceptor has a single vote round and vote value. With
  // Fast MultiPaxos, we have one pair of vote round and vote value per slot.
  // We call such a pair a vote. The vote also includes the highest round in
  // which an acceptor has received a distinguished "any" value.
  @JSExportAll
  sealed trait VoteValue
  case class VVCommand(command: Command) extends VoteValue
  case object VVNoop extends VoteValue
  case object VVNothing extends VoteValue

  @JSExportAll
  case class Entry(
      voteRound: Int,
      voteValue: VoteValue,
      anyRound: Option[Int]
  )

  // The log of votes.
  @JSExport
  protected val log: Log[Entry] = new Log()

  // If this acceptor receives a propose request from a client, it attempts to
  // choose the command in `nextSlot`.
  @JSExport
  protected var nextSlot = 0;

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import AcceptorInbound.Request
    inbound.request match {
      case Request.ProposeRequest(r) => handleProposeRequest(src, r)
      case Request.Phase1A(r)        => handlePhase1a(src, r)
      case Request.Phase2ABuffer(r)  => handlePhase2aBuffer(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  private def handleProposeRequest(
      src: Transport#Address,
      proposeRequest: ProposeRequest
  ): Unit = {
    metrics.requestsTotal.labels("ProposeRequest").inc()

    // If the wait period and wait stagger are both 0, then we don't bother
    // fiddling with a timer. We process the propose request immediately.
    if (options.waitPeriod == java.time.Duration.ofSeconds(0) &&
        options.waitStagger == java.time.Duration.ofSeconds(0)) {
      processProposeRequest(src, proposeRequest)
    } else {
      val t = (System.nanoTime(), src, proposeRequest)
      bufferedProposeRequests += t
    }
  }

  private def handlePhase1a(src: Transport#Address, phase1a: Phase1a): Unit = {
    metrics.requestsTotal.labels("Phase1a").inc()

    // Ignore messages from previous rounds.
    if (phase1a.round <= round) {
      logger.info(
        s"An acceptor received a phase 1a message for round " +
          s"${phase1a.round} but is in round $round."
      )
      val leader = chan[Leader[Transport]](src, Leader.serializer)
      leader.send(
        LeaderInbound().withPhase1BNack(
          Phase1bNack(acceptorId = acceptorId, round = round)
        )
      )
      return
    }

    // Bump our round and send the leader all of our votes. Note that we
    // exclude votes below the chosen watermark and votes for slots that the
    // leader knows are chosen. We also make sure not to return votes for slots
    // that we haven't actually voted in.
    round = phase1a.round
    val votes = log
      .prefix()
      .iteratorFrom(phase1a.chosenWatermark)
      .filter({ case (slot, _) => !phase1a.chosenSlot.contains(slot) })
      .flatMap({
        case (s, Entry(vr, VVCommand(command), _)) =>
          Some(Phase1bVote(slot = s, voteRound = vr).withCommand(command))
        case (s, Entry(vr, VVNoop, _)) =>
          Some(Phase1bVote(slot = s, voteRound = vr).withNoop(Noop()))
        case (_, Entry(_, VVNothing, _)) =>
          None
      })
    val leader = leaders(config.roundSystem.leader(round))
    leader.send(
      LeaderInbound().withPhase1B(
        Phase1b(acceptorId = acceptorId, round = round, vote = votes.to[Seq])
      )
    )
  }

  private def handlePhase2aBuffer(
      src: Transport#Address,
      phase2aBuffer: Phase2aBuffer
  ): Unit = {
    metrics.requestsTotal.labels("Phase2aBuffer").inc()
    val leader = leaders(config.roundSystem.leader(round))
    leader.send(
      LeaderInbound().withPhase2BBuffer(
        Phase2bBuffer(phase2aBuffer.phase2A.flatMap(processPhase2a))
      )
    )
  }

  // Methods ///////////////////////////////////////////////////////////////////
  private def processBufferedProposeRequests(): Unit = {
    val cutoff = System.nanoTime() - options.waitStagger.toNanos()
    val batch = bufferedProposeRequests.takeWhile({
      case (timestamp, _, _) => timestamp <= cutoff
    })
    bufferedProposeRequests = bufferedProposeRequests.drop(batch.size)
    metrics.batchesTotal.inc()
    metrics.proposeRequestsInBatchesTotal.inc(batch.size)

    val phase2bs = batch
      .sortBy({
        case (_, src, proposeRequest) => (src, proposeRequest).hashCode
      })
      .flatMap({
        case (_, src, proposeRequest) =>
          processProposeRequest(src, proposeRequest)
      })
    if (phase2bs.size > 0) {
      val leader = leaders(config.roundSystem.leader(round))
      leader.send(LeaderInbound().withPhase2BBuffer(Phase2bBuffer(phase2bs)))
    }
  }

  private def processProposeRequest(
      src: Transport#Address,
      proposeRequest: ProposeRequest
  ): Option[Phase2b] = {
    log.get(nextSlot) match {
      case Some(Entry(voteRound, _, Some(r))) if r == round && voteRound < r =>
        // If we previously received the distinguished "any" value in this
        // round and we have not already voted in this round, then we are free
        // to vote for the client's request.
        log.put(nextSlot, Entry(r, VVCommand(proposeRequest.command), None))
        nextSlot += 1
        Some(
          Phase2b(acceptorId = acceptorId, slot = nextSlot, round = round)
            .withCommand(proposeRequest.command)
        )

      case Some(_) | None =>
        // If we have not received the distinguished "any" value, then we
        // simply ignore the client's request.
        //
        // TODO(mwhittaker): Inform the client of the round, so that they can
        // send it to the leader.
        None
    }
  }

  private def processPhase2a(phase2a: Phase2a): Option[Phase2b] = {
    val Entry(voteRound, voteValue, anyRound) = log.get(phase2a.slot) match {
      case Some(vote) => vote
      case None       => Entry(-1, VVNothing, None)
    }

    // Ignore messages from smaller rounds.
    if (phase2a.round < round) {
      logger.debug(
        s"An acceptor received a phase 2a message for round " +
          s"${phase2a.round} but is in round $round."
      )
      return None
    }

    // Ignore messages from our current round if we've already voted. Though,
    // we do relay our vote again to the leader for liveness.
    if (phase2a.round == voteRound) {
      logger.check_gt(voteRound, -1)
      logger.debug(
        s"An acceptor received a phase 2a message for round " +
          s"${phase2a.round} but has already voted in round $round. The " +
          s"acceptor is relaying its vote again."
      )
      val leader = leaders(config.roundSystem.leader(round))
      var phase2b =
        Phase2b(acceptorId = acceptorId, slot = phase2a.slot, round = voteRound)
      return voteValue match {
        case VVCommand(command: Command) =>
          Some(phase2b.withCommand(command))
        case VVNoop =>
          Some(phase2b.withNoop(Noop()))
        case VVNothing =>
          logger.fatal("It's impossible for us to have voted for nothing.")
      }
    }

    // Update our round, vote round, vote value, next slot, and respond to the
    // leader.
    round = phase2a.round
    return phase2a.value match {
      case Phase2a.Value.Command(command) =>
        log.put(phase2a.slot, Entry(round, VVCommand(command), None))
        if (phase2a.slot >= nextSlot) {
          nextSlot = phase2a.slot + 1
        }
        Some(
          Phase2b(acceptorId = acceptorId, slot = phase2a.slot, round = round)
            .withCommand(command)
        )

      case Phase2a.Value.Noop(_) =>
        log.put(phase2a.slot, Entry(round, VVNoop, None))
        if (phase2a.slot >= nextSlot) {
          nextSlot = phase2a.slot + 1
        }
        Some(
          Phase2b(acceptorId = acceptorId, slot = phase2a.slot, round = round)
            .withNoop(Noop())
        )

      case Phase2a.Value.Any(_) =>
        log.put(phase2a.slot, Entry(voteRound, voteValue, Some(round)))
        None

      case Phase2a.Value.AnySuffix(_) =>
        if (log.prefix.size == 0) {
          log.putTail(phase2a.slot, Entry(-1, VVNothing, Some(round)))
        } else {
          val updatedVotes =
            log
              .prefix()
              .iteratorFrom(phase2a.slot)
              .map({
                case (s, Entry(vr, vv, _)) => (s, Entry(vr, vv, Some(round)))
              })
          for ((slot, entry) <- updatedVotes) {
            log.put(slot, entry)
          }
          log.putTail(log.prefix.lastKey + 1, Entry(-1, VVNothing, Some(round)))
        }
        None

      case Phase2a.Value.Empty =>
        logger.fatal("Empty Phase2a value.")
    }
  }
}
