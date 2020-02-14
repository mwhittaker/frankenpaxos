package frankenpaxos.matchmakermultipaxos

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
object MatchmakerInboundSerializer extends ProtoSerializer[MatchmakerInbound] {
  type A = MatchmakerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Matchmaker {
  val serializer = MatchmakerInboundSerializer
}

@JSExportAll
case class MatchmakerOptions(
    measureLatencies: Boolean
)

@JSExportAll
object MatchmakerOptions {
  val default = MatchmakerOptions(
    measureLatencies = true
  )
}

@JSExportAll
class MatchmakerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_matchmaker_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakermultipaxos_matchmaker_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val nacksSentTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_matchmaker_nacks_sent_total")
    .help("Total number of nacks sent.")
    .register()

  val staleGarbageCollectsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_matchmaker_stale_garbage_collects_total")
    .help("Total number of stale GarbageCollects received.")
    .register()
}

@JSExportAll
class Matchmaker[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: MatchmakerOptions = MatchmakerOptions.default,
    metrics: MatchmakerMetrics = new MatchmakerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.matchmakerAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = MatchmakerInbound
  override val serializer = MatchmakerInboundSerializer

  type Round = Int
  type Epoch = Int
  type ReconfigurerIndex = Int

  // A matchmaker's log looks something like this:
  //
  //     |   |
  //     +---+
  //   3 | T |
  //     +---+
  //   2 | S |
  //     +---+
  //   1 | R |
  //     +---+
  //   0 | Q |
  //     +---+
  //
  // It's a sequence of configurations indexed by round. Over time, leaders can
  // garbage collect prefixes of the log, so we maintain a gcWatermark. This
  // looks something like this:
  //
  //     |   |
  //     +---+
  //   3 | T |
  //     +---+
  //   2 | S |
  //     +---+
  //   1 | R |
  //     +---+ <-- gcWatermark (1)
  //   0 |###|
  //     +---+
  //
  // All entries below the gcWatermark are garbage collected.
  @JSExportAll
  case class Log(
      gcWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  )

  @JSExportAll
  sealed trait MatchmakerState

  @JSExportAll
  case class Pending(
      logs: mutable.Map[ReconfigurerIndex, Log]
  ) extends MatchmakerState

  @JSExportAll
  case class Normal(
      gcWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  ) extends MatchmakerState

  @JSExportAll
  case class HasStopped(
      gcWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  ) extends MatchmakerState

  @JSExportAll
  case class AcceptorState(
      round: Int,
      voteRound: Int,
      voteValue: Option[MatchmakerConfiguration]
  )

  // Fields ////////////////////////////////////////////////////////////////////
  private val index = config.matchmakerAddresses.indexOf(address)

  // Recall that we can reconfigure from one set of matchmakers to another. We
  // assign each set of matchmakers an epoch and reconfigure from one epoch to
  // the next. A physical matchmaker may play the role of multiple logical
  // matchmakers in different epochs.
  @JSExport
  protected var matchmakerStates = mutable.SortedMap[Epoch, MatchmakerState]()

  @JSExport
  protected var acceptorStates = mutable.SortedMap[Epoch, AcceptorState]()

  // For simplicity, we assume the first 2f+1 matchmakers are in the first
  // epoch.
  if (index < 2 * config.f + 1) {
    matchmakerStates(0) = Normal(
      gcWatermark = 0,
      configurations = mutable.SortedMap()
    )
    acceptorStates(0) =
      AcceptorState(round = -1, voteRound = -1, voteValue = None)
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

  private def transitionToHasStopped(
      epoch: Epoch,
      reconfigurerIndex: ReconfigurerIndex
  ): HasStopped = {
    matchmakerStates(epoch) match {
      case pending: Pending =>
        pending.logs.get(reconfigurerIndex) match {
          case None =>
            logger.fatal(
              s"A matchmaker was told to stop in epoch $epoch, " +
                s"reconfigurerIndex $reconfigurerIndex, but is still " +
                s"pending. This means that some matchmaker configuration " +
                s"and corresponding log was chosen in epoch $epoch (we " +
                s"can't stop an epoch before it starts), but we haven't " +
                s"learned that it was chosen until now. Before a " +
                s"configuration can be chosen, its log must be sent to all " +
                s"of the matchmakers. Thus, we must have the log stored here."
            )

          case Some(log) =>
            val hasStopped = HasStopped(
              gcWatermark = log.gcWatermark,
              configurations = log.configurations
            )
            matchmakerStates(epoch) = hasStopped
            hasStopped
        }

      case normal: Normal =>
        val hasStopped = HasStopped(
          gcWatermark = normal.gcWatermark,
          configurations = normal.configurations
        )
        matchmakerStates(epoch) = hasStopped
        hasStopped

      case hasStopped: HasStopped =>
        hasStopped
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import MatchmakerInbound.Request

    val label = inbound.request match {
      case Request.MatchRequest(_)   => "MatchRequest"
      case Request.GarbageCollect(_) => "GarbageCollect"
      case Request.Stop(_)           => "Stop"
      case Request.Bootstrap(_)      => "Bootstrap"
      case Request.MatchPhase1A(_)   => "MatchPhase1a"
      case Request.MatchPhase2A(_)   => "MatchPhase2a"
      case Request.MatchChosen(_)    => "MatchChosen"
      case Request.Empty =>
        logger.fatal("Empty MatchmakerInbound encountered.")
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.MatchRequest(r)   => handleMatchRequest(src, r)
        case Request.GarbageCollect(r) => handleGarbageCollect(src, r)
        case Request.Stop(r)           => handleStop(src, r)
        case Request.Bootstrap(r)      => handleBootstrap(src, r)
        case Request.MatchPhase1A(r)   => handleMatchPhase1a(src, r)
        case Request.MatchPhase2A(r)   => handleMatchPhase2a(src, r)
        case Request.MatchChosen(r)    => handleMatchChosen(src, r)
        case Request.Empty =>
          logger.fatal("Empty MatchmakerInbound encountered.")
      }
    }
  }

  private def handleMatchRequest(
      src: Transport#Address,
      matchRequest: MatchRequest
  ): Unit = {
    // It is an invariant that if a leader contacts a matchmaker in a given
    // epoch, then the matchmaker must know about the epoch. Initially this
    // true of epoch 0. For every later epoch, leaders will not send to the
    // epoch until the epoch's matchmakers have been chosen, and an epoch's
    // matchmakers won't be chosen until every matchmaker has been intialized
    // with the log of configurations and thus will know about the epoch.
    val epoch = matchRequest.matchmakerConfiguration.epoch
    logger.check(matchmakerStates.contains(epoch))

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    val normal = matchmakerStates(epoch) match {
      case pending: Pending =>
        // It's possible we haven't yet heard we've been chosen for this epoch
        // but we've received a message anyway. We only receive a MatchRequest
        // if we've been chosen, so we pretend as if we've just learned we've
        // been chosen.
        val reconfigurerIndex =
          matchRequest.matchmakerConfiguration.reconfigurerIndex
        pending.logs.get(reconfigurerIndex) match {
          case None =>
            logger.fatal(
              s"Matchmaker received a MatchRequest with " +
                s"matchmakerConfiguration from reconfigurer " +
                s"${reconfigurerIndex}, but it doesn't have a pending log " +
                s"from this reconfigurer. This should be impossible."
            )

          case Some(log) =>
            Normal(gcWatermark = log.gcWatermark,
                   configurations = log.configurations)
        }

      case normal: Normal => normal

      case stopped: HasStopped =>
        // If we're currently stopped, then we let the leader know. The leader
        // has to give up on this epoch and move to the next epoch.
        leader.send(LeaderInbound().withStopped(Stopped(epoch)))
        return
    }
    matchmakerStates(epoch) = normal

    // Recall that a matchmaker's log looks something like this:
    //
    //     |   |
    //     +---+
    //   3 | T |
    //     +---+
    //   2 | S |
    //     +---+
    //   1 | R |
    //     +---+ <-- gcWatermark
    //   0 |###|
    //     +---+
    //
    // If a MatchRequest's round is larger than any previously seen and larger
    // than the gcWatermark, then we can process it. If the MatchRequest's
    // round has already been processed, then a nack is sent.
    if (matchRequest.configuration.round < normal.gcWatermark) {
      logger.debug(
        s"Matchmaker received a MatchRequest in round " +
          s"${matchRequest.configuration.round} but has a gcWatermark of " +
          s"${normal.gcWatermark}, so the request is being ignored, and a " +
          s"nack is being sent."
      )
      leader.send(
        LeaderInbound()
          .withMatchmakerNack(MatchmakerNack(round = normal.gcWatermark - 1))
      )
      metrics.nacksSentTotal.inc()
      return
    }

    if (!normal.configurations.isEmpty &&
        matchRequest.configuration.round <= normal.configurations.lastKey) {
      logger.debug(
        s"Matchmaker received a MatchRequest in round " +
          s"${matchRequest.configuration.round} but has already processed a " +
          s"MatchRequest in round ${normal.configurations.lastKey}, so the " +
          s"request is being ignored, and a nack is being sent back."
      )
      leader.send(
        LeaderInbound()
          .withMatchmakerNack(
            MatchmakerNack(round = normal.configurations.lastKey)
          )
      )
      metrics.nacksSentTotal.inc()
      return
    }

    // Send back all previous configurations and store the (potentially new)
    // acceptor group.
    leader.send(
      LeaderInbound().withMatchReply(
        MatchReply(
          epoch = epoch,
          round = matchRequest.configuration.round,
          matchmakerIndex = index,
          gcWatermark = normal.gcWatermark,
          configuration = normal.configurations.values
            .takeWhile(_.round < matchRequest.configuration.round)
            .toSeq
        )
      )
    )

    normal.configurations(matchRequest.configuration.round) =
      matchRequest.configuration
    matchmakerStates(epoch) = normal
  }

  private def handleGarbageCollect(
      src: Transport#Address,
      garbageCollect: GarbageCollect
  ): Unit = {
    // It may not be strictly necessary, but we only process a GarbageCollect
    // command in the epoch that the leader intended. This simplifies things
    // quite a bit.
    val epoch = garbageCollect.matchmakerConfiguration.epoch
    if (!matchmakerStates.contains(epoch)) {
      return
    }

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    val normal: Normal = matchmakerStates(epoch) match {
      case pending: Pending =>
        val reconfigurerIndex =
          garbageCollect.matchmakerConfiguration.reconfigurerIndex
        pending.logs.get(reconfigurerIndex) match {
          case None =>
            logger.fatal(
              s"Matchmaker received a GarbageCollect with " +
                s"matchmakerConfiguration from reconfigurer " +
                s"${reconfigurerIndex}, but it doesn't have a pending log " +
                s"from this reconfigurer. This should be impossible."
            )

          case Some(log) =>
            Normal(gcWatermark = log.gcWatermark,
                   configurations = log.configurations)
        }

      case normal: Normal => normal

      case stopped: HasStopped =>
        leader.send(LeaderInbound().withStopped(Stopped(epoch)))
        return
    }

    // Send back an ack. Note that we don't ignore stale requests. If we did
    // this, then it's possible a leader with a stale gcWatermark would be
    // completely ignored by the acceptors, which is not what we want.
    val gcWatermark = Math.max(normal.gcWatermark, garbageCollect.gcWatermark)
    leader.send(
      LeaderInbound().withGarbageCollectAck(
        GarbageCollectAck(
          epoch = epoch,
          matchmakerIndex = index,
          gcWatermark = gcWatermark
        )
      )
    )

    // Garbage collect configurations.
    val configurations = normal.configurations.dropWhile({
      case (round, _) => round < gcWatermark
    })
    matchmakerStates(epoch) = normal.copy(
      gcWatermark = gcWatermark,
      configurations = configurations
    )
  }

  private def handleStop(src: Transport#Address, stop: Stop): Unit = {
    val epoch = stop.matchmakerConfiguration.epoch
    logger.check(matchmakerStates.contains(epoch))

    val hasStopped = transitionToHasStopped(
      epoch,
      stop.matchmakerConfiguration.reconfigurerIndex
    )
    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    reconfigurer.send(
      ReconfigurerInbound().withStopAck(
        StopAck(matchmakerIndex = index,
                epoch = epoch,
                gcWatermark = hasStopped.gcWatermark,
                configuration = hasStopped.configurations.values.toSeq)
      )
    )
  }

  private def handleBootstrap(
      src: Transport#Address,
      bootstrap: Bootstrap
  ): Unit = {
    matchmakerStates.get(bootstrap.epoch) match {
      case None =>
        matchmakerStates(bootstrap.epoch) = Pending(
          mutable.Map(
            bootstrap.reconfigurerIndex ->
              Log(
                gcWatermark = bootstrap.gcWatermark,
                configurations = mutable.SortedMap() ++
                  bootstrap.configuration.map(c => c.round -> c)
              )
          )
        )
        acceptorStates(bootstrap.epoch) =
          AcceptorState(round = -1, voteRound = -1, voteValue = None)

      case Some(pending: Pending) =>
        pending.logs(bootstrap.reconfigurerIndex) = Log(
          gcWatermark = bootstrap.gcWatermark,
          configurations = mutable.SortedMap() ++
            bootstrap.configuration.map(c => c.round -> c)
        )
        logger.check(acceptorStates.contains(bootstrap.epoch))

      case Some(_: Normal) | Some(_: HasStopped) =>
        logger.debug(
          s"Matchmaker received a Bootstrap request in epoch " +
            s"${bootstrap.epoch}, but is already in state " +
            s"${matchmakerStates.get(bootstrap.epoch)}. The state " +
            s"is not changed, but for liveness, an ack is being sent back."
        )
    }

    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    reconfigurer.send(
      ReconfigurerInbound().withBootstrapAck(
        BootstrapAck(matchmakerIndex = index, epoch = bootstrap.epoch)
      )
    )
  }

  private def handleMatchPhase1a(
      src: Transport#Address,
      matchPhase1a: MatchPhase1a
  ): Unit = {
    val epoch = matchPhase1a.matchmakerConfiguration.epoch
    logger.check(matchmakerStates.contains(epoch))
    logger.check(acceptorStates.contains(epoch))

    // Update our matchmaker state. If we've entered Phase 1, we should be
    // stopped, though we may not have received a Stop.
    transitionToHasStopped(
      epoch,
      matchPhase1a.matchmakerConfiguration.reconfigurerIndex
    )

    // Update our acceptor state.
    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    val acceptorState = acceptorStates(epoch)
    // If we receive an out of date round, we send back a nack.
    if (matchPhase1a.round < acceptorState.round) {
      logger.debug(
        s"Matchmaker received a MatchPhase1a message in round " +
          s"${matchPhase1a.round} but is in round ${acceptorState.round}. " +
          s"The matchmaker is sending back a nack."
      )
      reconfigurer.send(
        ReconfigurerInbound()
          .withMatchNack(
            MatchNack(epoch = epoch, round = acceptorState.round)
          )
      )
      return
    }

    // Otherwise, we update our round and send back a Phase1b message to the
    // reconfigurer.
    reconfigurer.send(
      ReconfigurerInbound().withMatchPhase1B(
        MatchPhase1b(
          epoch = epoch,
          round = matchPhase1a.round,
          matchmakerIndex = index,
          vote = acceptorState.voteValue.map(
            v =>
              MatchPhase1bVote(voteRound = acceptorState.voteRound,
                               voteValue = v)
          )
        )
      )
    )
    acceptorStates(epoch) = acceptorState.copy(round = matchPhase1a.round)
  }

  private def handleMatchPhase2a(
      src: Transport#Address,
      matchPhase2a: MatchPhase2a
  ): Unit = {
    val epoch = matchPhase2a.matchmakerConfiguration.epoch
    logger.check(matchmakerStates.contains(epoch))
    logger.check(acceptorStates.contains(epoch))

    // Update our matchmaker state. If we've entered Phase 2, we should be
    // stopped, though we may not have received a Stop.
    transitionToHasStopped(
      epoch,
      matchPhase2a.matchmakerConfiguration.reconfigurerIndex
    )

    // Update our acceptor state.
    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    val acceptorState = acceptorStates(epoch)
    // If we receive an out of date round, we send back a nack to the
    // leader.
    if (matchPhase2a.round < acceptorState.round) {
      logger.debug(
        s"Matchmaker received a MatchPhase2a message in round " +
          s"${matchPhase2a.round} but is in round ${acceptorState.round}. " +
          s"The acceptor is sending back a nack."
      )
      reconfigurer.send(
        ReconfigurerInbound()
          .withMatchNack(
            MatchNack(epoch = epoch, round = acceptorState.round)
          )
      )
      return
    }

    // Otherwise, update our round and send back a Phase2b to the leader.
    reconfigurer.send(
      ReconfigurerInbound().withMatchPhase2B(
        MatchPhase2b(epoch = epoch,
                     round = matchPhase2a.round,
                     matchmakerIndex = index)
      )
    )

    acceptorStates(epoch) = acceptorState.copy(
      round = matchPhase2a.round,
      voteRound = matchPhase2a.round,
      voteValue = Some(matchPhase2a.value)
    )
  }

  private def handleMatchChosen(
      src: Transport#Address,
      matchChosen: MatchChosen
  ): Unit = {
    val epoch = matchChosen.value.epoch
    logger.check(matchmakerStates.contains(epoch))
    matchmakerStates(epoch) match {
      case pending: Pending =>
        val reconfigurerIndex = matchChosen.value.reconfigurerIndex
        pending.logs.get(reconfigurerIndex) match {
          case None =>
            logger.fatal(
              s"Matchmaker received a MatchChosen with " +
                s"matchmakerConfiguration from reconfigurer " +
                s"${reconfigurerIndex}, but it doesn't have a pending log " +
                s"from this reconfigurer. This should be impossible."
            )

          case Some(log) =>
            matchmakerStates(epoch) = Normal(
              gcWatermark = log.gcWatermark,
              configurations = log.configurations
            )
        }

      case _: Normal | _: HasStopped =>
        // Do nothing.
        ()
    }
  }
}
