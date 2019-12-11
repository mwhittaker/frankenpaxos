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

  sealed trait MatchmakerState

  case class Pending(
      gcWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  ) extends MatchmakerState

  case class Normal(
      gcWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  ) extends MatchmakerState

  case class Stopped(
      gcWatermark: Int,
      configurations: mutable.SortedMap[Round, Configuration]
  ) extends MatchmakerState

  sealed trait AcceptorState

  case class NotChosen(
      round: Int,
      voteRound: Int,
      voteValue: Option[MatchmakerConfiguration]
  ) extends AcceptorState

  case class Chosen(value: MatchmakerConfiguration) extends AcceptorState

// Fields ////////////////////////////////////////////////////////////////////
  private val index = config.matchmakerAddresses.indexOf(address)

  @JSExport
  protected var matchmakerStates = mutable.SortedMap[Epoch, MatchmakerState]()

  @JSExport
  protected var acceptorStates = mutable.SortedMap[Epoch, AcceptorState]()

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
      case Request.MatchPhase1A(_)   => "McPtaphase1a"
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
    logger.check(matchmakerStates.contains(matchRequest.epoch))

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    val normal = matchmakerStates(matchRequest.epoch) match {
      case pending: Pending =>
        // It's possible we haven't yet heard we've been chosen for this epoch
        // but we've received a message anyway. We only receive a MatchRequest
        // if we've been chosen, so we pretend as if we've just learned we've
        // been chosen.
        Normal(gcWatermark = pending.gcWatermark,
               configurations = pending.configurations)

      case normal: Normal => normal

      case stopped: Stopped =>
        // If we're currently stopped, then the leader has to give up on this
        // epoch and move to the next epoch. If we know the next epoch, we let
        // the leader know what it is. Otherwise, we just let them know that
        // we're stopped.
        acceptorStates(matchRequest.epoch) match {
          case _: NotChosen =>
            leader.send(LeaderInbound().withIsStopped(IsStopped()))
          case chosen: Chosen =>
            leader.send(
              LeaderInbound().withMatchChosen(
                MatchChosen(epoch = matchRequest.epoch, value = chosen.value)
              )
            )
        }
        return
    }

    // A matchmaker only processes a match request if the request's round is
    // larger than any previously seen and at least as large as the
    // gcWatermark. Otherwise, a nack is sent back.
    //
    // It's possible to implement things so that leaders re-send MatchRequests
    // if they haven't received a MatchReply for a while. If we did this, then
    // the matchmaker would have to re-send replies to requests that it has
    // already processed. We don't do this though.
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

    // Send back all previous acceptor groups and store the new acceptor group.
    leader.send(
      LeaderInbound().withMatchReply(
        MatchReply(
          epoch = matchRequest.epoch,
          round = matchRequest.configuration.round,
          matchmakerIndex = index,
          gcWatermark = normal.gcWatermark,
          configuration = normal.configurations.values.toSeq
        )
      )
    )

    normal.configurations(matchRequest.configuration.round) =
      matchRequest.configuration
    matchmakerStates(matchRequest.epoch) = normal
  }

  private def handleGarbageCollect(
      src: Transport#Address,
      garbageCollect: GarbageCollect
  ): Unit = {
    logger.check(matchmakerStates.contains(garbageCollect.epoch))

    val leader = chan[Leader[Transport]](src, Leader.serializer)
    val normal: Normal = matchmakerStates(garbageCollect.epoch) match {
      case pending: Pending =>
        Normal(gcWatermark = pending.gcWatermark,
               configurations = pending.configurations)

      case normal: Normal => normal

      case stopped: Stopped =>
        acceptorStates(garbageCollect.epoch) match {
          case _: NotChosen =>
            leader.send(LeaderInbound().withIsStopped(IsStopped()))
          case chosen: Chosen =>
            leader.send(
              LeaderInbound().withMatchChosen(
                MatchChosen(epoch = garbageCollect.epoch, value = chosen.value)
              )
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
          epoch = garbageCollect.epoch,
          matchmakerIndex = index,
          gcWatermark = gcWatermark
        )
      )
    )

    // Garbage collect configurations.
    val configurations = normal.configurations.dropWhile({
      case (round, _) => round < gcWatermark
    })
    matchmakerStates(garbageCollect.epoch) = normal.copy(
      gcWatermark = gcWatermark,
      configurations = configurations
    )
  }

  private def handleStop(src: Transport#Address, stop: Stop): Unit = {
    logger.check(matchmakerStates.contains(stop.epoch))

    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)
    matchmakerStates(stop.epoch) match {
      case _: Pending =>
        logger.fatal(
          s"Matchmaker received a Stop request while pending. This is " +
            s"erroneous behavior. We should only stop a set of matchmakers " +
            "if they have begun executing. There must be a bug!"
        )

      case normal: Normal =>
        reconfigurer.send(
          ReconfigurerInbound().withStopAck(
            StopAck(matchmakerIndex = index,
                    epoch = stop.epoch,
                    gcWatermark = normal.gcWatermark,
                    configuration = normal.configurations.values.toSeq)
          )
        )
        matchmakerStates(stop.epoch) = Stopped(gcWatermark = normal.gcWatermark,
                                               configurations =
                                                 normal.configurations)

      case stopped: Stopped =>
        // Note that we're already stopped, but for liveness we send back a
        // StopAck anyway. Sending redundant StopAcks is safe.
        reconfigurer.send(
          ReconfigurerInbound().withStopAck(
            StopAck(matchmakerIndex = index,
                    epoch = stop.epoch,
                    gcWatermark = stopped.gcWatermark,
                    configuration = stopped.configurations.values.toSeq)
          )
        )
    }
  }

  private def handleBootstrap(
      src: Transport#Address,
      bootstrap: Bootstrap
  ): Unit = {
    matchmakerStates.get(bootstrap.epoch) match {
      case None =>
        matchmakerStates(bootstrap.epoch) = Pending(
          gcWatermark = bootstrap.gcWatermark,
          configurations = mutable.SortedMap() ++
            bootstrap.configuration.map(c => c.round -> c)
        )

      case Some(state) =>
        logger.debug(
          s"Matchmaker received a Bootstrap request in epoch " +
            s"${bootstrap.epoch}, but is already in state $state. The state " +
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
    logger.check(acceptorStates.contains(matchPhase1a.epoch))

    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)

    acceptorStates(matchPhase1a.epoch) match {
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
              .withMatchNack(MatchNack(round = notChosen.round))
          )
          return
        }

        // Otherwise, we update our round and send back a Phase1b message to the
        // reconfigurer.
        reconfigurer.send(
          ReconfigurerInbound().withMatchPhase1B(
            MatchPhase1b(
              epoch = matchPhase1a.epoch,
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
        acceptorStates(matchPhase1a.epoch) =
          notChosen.copy(round = matchPhase1a.round)

      case chosen: Chosen =>
        reconfigurer.send(
          ReconfigurerInbound()
            .withMatchChosen(
              MatchChosen(epoch = matchPhase1a.epoch, value = chosen.value)
            )
        )
    }
  }

  private def handleMatchPhase2a(
      src: Transport#Address,
      matchPhase2a: MatchPhase2a
  ): Unit = {
    logger.check(acceptorStates.contains(matchPhase2a.epoch))

    val reconfigurer =
      chan[Reconfigurer[Transport]](src, Reconfigurer.serializer)

    acceptorStates(matchPhase2a.epoch) match {
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
              .withMatchNack(MatchNack(round = notChosen.round))
          )
          return
        }

        // Otherwise, update our round and send back a Phase2b to the leader.
        reconfigurer.send(
          ReconfigurerInbound()
            .withMatchPhase2B(
              MatchPhase2b(epoch = matchPhase2a.epoch,
                           round = matchPhase2a.round,
                           matchmakerIndex = index)
            )
        )

        acceptorStates(matchPhase2a.epoch) = notChosen.copy(
          round = matchPhase2a.round,
          voteRound = matchPhase2a.round,
          voteValue = Some(matchPhase2a.value)
        )

      case chosen: Chosen =>
        reconfigurer.send(
          ReconfigurerInbound()
            .withMatchChosen(
              MatchChosen(epoch = matchPhase2a.epoch, value = chosen.value)
            )
        )
    }
  }

  private def handleMatchChosen(
      src: Transport#Address,
      matchChosen: MatchChosen
  ): Unit = {
    // Update the epoch in which the value was chosen. Stopping the matchmaker
    // isn't strictly required, but it makes sense to do. It can only help.
    acceptorStates(matchChosen.epoch) = Chosen(matchChosen.value)
    logger.check(matchmakerStates.contains(matchChosen.epoch))
    matchmakerStates(matchChosen.epoch) match {
      case pending: Pending =>
        matchmakerStates(matchChosen.epoch) =
          Stopped(pending.gcWatermark, pending.configurations)
      case normal: Normal =>
        matchmakerStates(matchChosen.epoch) =
          Stopped(normal.gcWatermark, normal.configurations)
      case _: Stopped =>
        // Do nothing.
        ()
    }

    // Update the chosen epoch.
    logger.check(matchmakerStates.contains(matchChosen.value.epoch))
    matchmakerStates(matchChosen.value.epoch) match {
      case pending: Pending =>
        matchmakerStates(matchChosen.value.epoch) =
          Normal(pending.gcWatermark, pending.configurations)
      case _: Normal | _: Stopped =>
        // Do nothing.
        ()
    }
  }
}
