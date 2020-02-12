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
  // All entries below the gcWatermark are garbage collected. Moreover, when we
  // reconfigure from one set of matchmakers to another, the new matchmakers
  // begin with the same exact log. These initial entries all lie below a
  // reconfigureWatermark. That looks something like this:
  //
  //     |   |
  //     +---+
  //   3 | T |
  //     +---+ <-- reconfigureWatermark (3)
  //   2 | S |
  //     +---+
  //   1 | R |
  //     +---+ <-- gcWatermark (1)
  //   0 |###|
  //     +---+
  //
  // MatchRequests for entries below the reconfigureWatermark are nacked.
  @JSExportAll
  case class Log(
      gcWatermark: Int,
      reconfigureWatermark: Int,
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
      reconfigureWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  ) extends MatchmakerState

  @JSExportAll
  case class HasStopped(
      gcWatermark: Int,
      reconfigureWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  ) extends MatchmakerState

  @JSExportAll
  sealed trait AcceptorState

  @JSExportAll
  case class NotChosen(
      round: Int,
      voteRound: Int,
      voteValue: Option[MatchmakerConfiguration]
  ) extends AcceptorState

  @JSExportAll
  case class YesChosen(value: MatchmakerConfiguration) extends AcceptorState

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
      reconfigureWatermark = 0,
      configurations = mutable.SortedMap()
    )
    acceptorStates(0) = NotChosen(round = -1, voteRound = -1, voteValue = None)
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
                   reconfigureWatermark = log.reconfigureWatermark,
                   configurations = log.configurations)
        }

      case normal: Normal => normal

      case stopped: HasStopped =>
        // If we're currently stopped, then the leader has to give up on this
        // epoch and move to the next epoch. If we know the next epoch, we let
        // the leader know what it is. Otherwise, we just let them know that
        // we're stopped.
        //
        // TODO(mwhittaker): We could inform the leader of the largest epoch we
        // know about, but it's probably not worth the optimization.
        acceptorStates(epoch) match {
          case _: NotChosen =>
            leader.send(
              LeaderInbound().withStopped(Stopped(epoch))
            )

          case chosen: YesChosen =>
            leader.send(
              LeaderInbound().withMatchChosen(MatchChosen(value = chosen.value))
            )
        }
        return
    }
    matchmakerStates(epoch) = normal

    // TODO(mwhittaker): Don't do re-sends?
    //
    // Recall that a matchmaker's log looks something like this:
    //
    //     |   |
    //     +---+
    //   3 | T |
    //     +---+ <-- reconfigureWatermark
    //   2 | S |
    //     +---+
    //   1 | R |
    //     +---+ <-- gcWatermark
    //   0 |###|
    //     +---+
    //
    // If a MatchRequest's round is larger than any previously seen, larger
    // than the reconfigureWatermark, and larger than the gcWatermark, then we
    // can process it.
    //
    // If the MatchRequest's round has already been processed, then the
    // Matchmaker resends the response, unless the round is below the
    // reconfigureWatermark or below the gcWatermark. In these cases, a nack is
    // sent back instead.
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

    if (matchRequest.configuration.round < normal.reconfigureWatermark) {
      logger.debug(
        s"Matchmaker received a MatchRequest in round " +
          s"${matchRequest.configuration.round} but has a " +
          s"reconfigureWatermark of ${normal.reconfigureWatermark}, so the " +
          s"request is being ignored, and a nack is being sent."
      )
      leader.send(
        LeaderInbound()
          .withMatchmakerNack(
            MatchmakerNack(round = normal.reconfigureWatermark - 1)
          )
      )
      metrics.nacksSentTotal.inc()
      return
    }

    // TODO(mwhittaker): Check to see if the round is less than the largest
    // round we have. If it is, we have to reject it. How did this get lost? I
    // thought I had code for this. Woops.

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
                   reconfigureWatermark = log.reconfigureWatermark,
                   configurations = log.configurations)
        }

      case normal: Normal => normal

      case stopped: HasStopped =>
        acceptorStates(epoch) match {
          case _: NotChosen =>
            leader.send(LeaderInbound().withStopped(Stopped(epoch)))
          case chosen: YesChosen =>
            leader.send(
              LeaderInbound().withMatchChosen(MatchChosen(value = chosen.value))
            )
        }
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

    val (watermark, configs) = matchmakerStates(epoch) match {
      case pending: Pending =>
        val reconfigurerIndex =
          stop.matchmakerConfiguration.reconfigurerIndex
        pending.logs.get(reconfigurerIndex) match {
          case None =>
            logger.fatal(
              s"Matchmaker received a Stop with matchmakerConfiguration " +
                s"from reconfigurer ${reconfigurerIndex}, but it doesn't " +
                s"have a pending log from this reconfigurer. This should " +
                s"be impossible."
            )

          case Some(log) =>
            matchmakerStates(epoch) = HasStopped(
              gcWatermark = log.gcWatermark,
              reconfigureWatermark = log.reconfigureWatermark,
              configurations = log.configurations
            )
            (log.reconfigureWatermark, log.configurations)
        }

      case normal: Normal =>
        matchmakerStates(epoch) = HasStopped(
          gcWatermark = normal.gcWatermark,
          reconfigureWatermark = normal.reconfigureWatermark,
          configurations = normal.configurations
        )
        (normal.gcWatermark, normal.configurations)

      case stopped: HasStopped =>
        // Note that we're already stopped, but for liveness we send back a
        // StopAck anyway. Sending redundant StopAcks is safe.
        (stopped.gcWatermark, stopped.configurations)
    }

    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    reconfigurer.send(
      ReconfigurerInbound().withStopAck(
        StopAck(matchmakerIndex = index,
                epoch = epoch,
                gcWatermark = watermark,
                configuration = configs.values.toSeq)
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
                reconfigureWatermark = bootstrap.configuration.size,
                configurations = mutable.SortedMap() ++
                  bootstrap.configuration.map(c => c.round -> c)
              )
          )
        )
        acceptorStates(bootstrap.epoch) =
          NotChosen(round = -1, voteRound = -1, voteValue = None)

      case Some(pending: Pending) =>
        pending.logs(bootstrap.reconfigurerIndex) = Log(
          gcWatermark = bootstrap.gcWatermark,
          reconfigureWatermark = bootstrap.configuration.size,
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
    matchmakerStates(epoch) match {
      case pending: Pending =>
        val reconfigurerIndex =
          matchPhase1a.matchmakerConfiguration.reconfigurerIndex
        pending.logs.get(reconfigurerIndex) match {
          case None =>
            logger.fatal(
              s"Matchmaker received a MatchPhase1a with " +
                s"matchmakerConfiguration from reconfigurer " +
                s"${reconfigurerIndex}, but it doesn't have a pending log " +
                s"from this reconfigurer. This should be impossible."
            )

          case Some(log) =>
            matchmakerStates(epoch) = HasStopped(
              gcWatermark = log.gcWatermark,
              reconfigureWatermark = log.reconfigureWatermark,
              configurations = log.configurations
            )
        }

      case normal: Normal =>
        matchmakerStates(epoch) = HasStopped(
          gcWatermark = normal.gcWatermark,
          reconfigureWatermark = normal.reconfigureWatermark,
          configurations = normal.configurations
        )

      case stopped: HasStopped =>
    }

    // Update our acceptor state.
    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    acceptorStates(epoch) match {
      case notChosen: NotChosen =>
        // If we receive an out of date round, we send back a nack.
        if (matchPhase1a.round < notChosen.round) {
          logger.debug(
            s"Matchmaker received a MatchPhase1a message in round " +
              s"${matchPhase1a.round} but is in round ${notChosen.round}. " +
              s"The matchmaker is sending back a nack."
          )
          reconfigurer.send(
            ReconfigurerInbound()
              .withMatchNack(
                MatchNack(epoch = epoch, round = notChosen.round)
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
              vote = notChosen.voteValue.map(
                v =>
                  MatchPhase1bVote(voteRound = notChosen.voteRound,
                                   voteValue = v)
              )
            )
          )
        )
        acceptorStates(epoch) = notChosen.copy(round = matchPhase1a.round)

      case chosen: YesChosen =>
        reconfigurer.send(
          ReconfigurerInbound()
            .withMatchChosen(MatchChosen(value = chosen.value))
        )
    }
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
    matchmakerStates(epoch) match {
      case pending: Pending =>
        val reconfigurerIndex =
          matchPhase2a.matchmakerConfiguration.reconfigurerIndex
        pending.logs.get(reconfigurerIndex) match {
          case None =>
            logger.fatal(
              s"Matchmaker received a MatchPhase2a with " +
                s"matchmakerConfiguration from reconfigurer " +
                s"${reconfigurerIndex}, but it doesn't have a pending log " +
                s"from this reconfigurer. This should be impossible."
            )

          case Some(log) =>
            matchmakerStates(epoch) = HasStopped(
              gcWatermark = log.gcWatermark,
              reconfigureWatermark = log.reconfigureWatermark,
              configurations = log.configurations
            )
        }

      case normal: Normal =>
        matchmakerStates(epoch) = HasStopped(
          gcWatermark = normal.gcWatermark,
          reconfigureWatermark = normal.reconfigureWatermark,
          configurations = normal.configurations
        )

      case stopped: HasStopped =>
    }

    // Update our acceptor state.
    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    acceptorStates(epoch) match {
      case notChosen: NotChosen =>
        // If we receive an out of date round, we send back a nack to the
        // leader.
        if (matchPhase2a.round < notChosen.round) {
          logger.debug(
            s"Matchmaker received a MatchPhase2a message in round " +
              s"${matchPhase2a.round} but is in round ${notChosen.round}. " +
              s"The acceptor is sending back a nack."
          )
          reconfigurer.send(
            ReconfigurerInbound()
              .withMatchNack(
                MatchNack(epoch = epoch, round = notChosen.round)
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

        acceptorStates(epoch) = notChosen.copy(
          round = matchPhase2a.round,
          voteRound = matchPhase2a.round,
          voteValue = Some(matchPhase2a.value)
        )

      case chosen: YesChosen =>
        reconfigurer.send(
          ReconfigurerInbound()
            .withMatchChosen(MatchChosen(value = chosen.value))
        )
    }
  }

  private def handleMatchChosen(
      src: Transport#Address,
      matchChosen: MatchChosen
  ): Unit = {

    // Update the epoch in which the value was chosen.
    val oldEpoch = matchChosen.value.epoch - 1
    if (matchmakerStates.contains(oldEpoch)) {
      logger.check(acceptorStates.contains(oldEpoch))

      matchmakerStates(oldEpoch) match {
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
              matchmakerStates(oldEpoch) = HasStopped(
                gcWatermark = log.gcWatermark,
                reconfigureWatermark = log.reconfigureWatermark,
                configurations = log.configurations
              )
          }

        case normal: Normal =>
          matchmakerStates(oldEpoch) = HasStopped(
            gcWatermark = normal.gcWatermark,
            reconfigureWatermark = normal.reconfigureWatermark,
            configurations = normal.configurations
          )

        case _: HasStopped =>
          // Do nothing.
          ()
      }

      acceptorStates(oldEpoch) = YesChosen(matchChosen.value)
    }

    // Update the chosen epoch.
    val newEpoch = matchChosen.value.epoch
    if (matchmakerStates.contains(newEpoch)) {
      matchmakerStates(newEpoch) match {
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
              matchmakerStates(oldEpoch) = HasStopped(
                gcWatermark = log.gcWatermark,
                reconfigureWatermark = log.reconfigureWatermark,
                configurations = log.configurations
              )
          }

        case _: Normal | _: HasStopped =>
          // Do nothing.
          ()
      }
    }
  }
}
