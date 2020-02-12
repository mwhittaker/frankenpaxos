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
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.quorums.QuorumSystem
import frankenpaxos.quorums.QuorumSystemProto
import frankenpaxos.quorums.SimpleMajority
import frankenpaxos.quorums.UnanimousWrites
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object ReconfigurerInboundSerializer
    extends ProtoSerializer[ReconfigurerInbound] {
  type A = ReconfigurerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Reconfigurer {
  val serializer = ReconfigurerInboundSerializer
}

@JSExportAll
case class ReconfigurerOptions(
    resendStopsPeriod: java.time.Duration,
    resendBootstrapsPeriod: java.time.Duration,
    resendMatchPhase1asPeriod: java.time.Duration,
    resendMatchPhase2asPeriod: java.time.Duration,
    measureLatencies: Boolean
)

@JSExportAll
object ReconfigurerOptions {
  val default = ReconfigurerOptions(
    resendStopsPeriod = java.time.Duration.ofSeconds(5),
    resendBootstrapsPeriod = java.time.Duration.ofSeconds(5),
    resendMatchPhase1asPeriod = java.time.Duration.ofSeconds(5),
    resendMatchPhase2asPeriod = java.time.Duration.ofSeconds(5),
    measureLatencies = true
  )
}

@JSExportAll
class ReconfigurerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_reconfigurer_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakermultipaxos_reconfigurer_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val resendStopsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_reconfigurer_resend_stops_total")
    .help("Total number of times the leader resent Stop messages.")
    .register()

  val resendBootstrapsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_reconfigurer_resend_bootstraps_total")
    .help("Total number of times the leader resent Bootstrap messages.")
    .register()

  val resendMatchPhase1asTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_reconfigurer_resend_matchPhase1as_total")
    .help("Total number of times the leader resent MatchPhase1a messages.")
    .register()

  val resendMatchPhase2asTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_reconfigurer_resend_matchPhase2as_total")
    .help("Total number of times the leader resent MatchPhase2a messages.")
    .register()
}

@JSExportAll
class Reconfigurer[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ReconfigurerOptions = ReconfigurerOptions.default,
    metrics: ReconfigurerMetrics = new ReconfigurerMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.reconfigurerAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReconfigurerInbound
  override val serializer = ReconfigurerInboundSerializer

  type Epoch = Int
  type MatchmakerIndex = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case class Idle(
      configuration: MatchmakerConfiguration
  ) extends State

  @JSExportAll
  case class Stopping(
      configuration: MatchmakerConfiguration,
      newConfiguration: MatchmakerConfiguration,
      stopAcks: mutable.Map[MatchmakerIndex, StopAck],
      resendStops: Transport#Timer
  ) extends State

  @JSExportAll
  case class Bootstrapping(
      configuration: MatchmakerConfiguration,
      newConfiguration: MatchmakerConfiguration,
      bootstrapAcks: mutable.Map[MatchmakerIndex, BootstrapAck],
      resendBootstraps: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase1(
      configuration: MatchmakerConfiguration,
      newConfiguration: MatchmakerConfiguration,
      round: Int,
      matchPhase1bs: mutable.Map[MatchmakerIndex, MatchPhase1b],
      resendMatchPhase1as: Transport#Timer
  ) extends State

  @JSExportAll
  case class Phase2(
      configuration: MatchmakerConfiguration,
      newConfiguration: MatchmakerConfiguration,
      round: Int,
      matchPhase2bs: mutable.Map[MatchmakerIndex, MatchPhase2b],
      resendMatchPhase2as: Transport#Timer
  ) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  private val index = config.reconfigurerAddresses.indexOf(address)

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Other Recofingurer channels.
  private val otherReconfigurers: Seq[Chan[Reconfigurer[Transport]]] =
    for (a <- config.reconfigurerAddresses if a != address)
      yield chan[Reconfigurer[Transport]](a, Reconfigurer.serializer)

  // Matchmaker channels.
  private val matchmakers: Seq[Chan[Matchmaker[Transport]]] =
    for (a <- config.matchmakerAddresses)
      yield chan[Matchmaker[Transport]](a, Matchmaker.serializer)

  // For simplicity, we use a round robin round system for the reconfigurers.
  private val roundSystem =
    new RoundSystem.ClassicRoundRobin(config.numReconfigurers)

  var state: State = Idle(
    MatchmakerConfiguration(
      epoch = 0,
      reconfigurerIndex = -1,
      matchmakerIndex = Seq() ++ (0 until (2 * config.f + 1))
    )
  )

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendStopsTimer(
      stop: Stop,
      matchmakerIndices: Set[MatchmakerIndex]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendStops",
      options.resendStopsPeriod,
      () => {
        metrics.resendStopsTotal.inc()
        for (index <- matchmakerIndices) {
          matchmakers(index).send(MatchmakerInbound().withStop(stop))
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendBootstrapsTimer(
      bootstrap: Bootstrap,
      matchmakerIndices: Set[MatchmakerIndex]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendBootstraps",
      options.resendBootstrapsPeriod,
      () => {
        metrics.resendBootstrapsTotal.inc()
        for (index <- matchmakerIndices) {
          matchmakers(index).send(MatchmakerInbound().withBootstrap(bootstrap))
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendMatchPhase1asTimer(
      matchPhase1a: MatchPhase1a,
      matchmakerIndices: Set[MatchmakerIndex]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendMatchPhase1as",
      options.resendMatchPhase1asPeriod,
      () => {
        metrics.resendMatchPhase1asTotal.inc()
        for (index <- matchmakerIndices) {
          matchmakers(index).send(
            MatchmakerInbound().withMatchPhase1A(matchPhase1a)
          )
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendMatchPhase2asTimer(
      matchPhase2a: MatchPhase2a,
      matchmakerIndices: Set[MatchmakerIndex]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendMatchPhase2as",
      options.resendMatchPhase2asPeriod,
      () => {
        metrics.resendMatchPhase2asTotal.inc()
        for (index <- matchmakerIndices) {
          matchmakers(index).send(
            MatchmakerInbound().withMatchPhase2A(matchPhase2a)
          )
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

  // Reconfigure is like `handleReconfigure` except we don't assume it comes
  // from a leader. This is mostly used to trigger reconfigurations manually
  // from the JS visualization.
  private def reconfigure(matchmakerIndices: Set[MatchmakerIndex]): Unit = {
    state match {
      case idle: Idle =>
        // Sends stops to the matchmakers.
        val stop = Stop(idle.configuration)
        for (index <- idle.configuration.matchmakerIndex) {
          matchmakers(index).send(MatchmakerInbound().withStop(stop))
        }

        // Update our state.
        state = Stopping(
          configuration = idle.configuration,
          newConfiguration = MatchmakerConfiguration(
            epoch = idle.configuration.epoch + 1,
            reconfigurerIndex = index,
            matchmakerIndex = matchmakerIndices.toSeq
          ),
          stopAcks = mutable.Map(),
          resendStops =
            makeResendStopsTimer(stop, idle.configuration.matchmakerIndex.toSet)
        )

      case _: Stopping | _: Bootstrapping | _: Phase1 | _: Phase2 =>
        logger.debug(
          s"Reconfigurer received a Reconfigurer command while it is " +
            s"reconfiguring. Its state is $state. The Reconfigure command is " +
            s"being ignored."
        )
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ReconfigurerInbound.Request

    val label =
      inbound.request match {
        case Request.Reconfigure(_)  => "Reconfigure"
        case Request.StopAck(_)      => "StopAck"
        case Request.BootstrapAck(_) => "BootstrapAck"
        case Request.MatchPhase1B(_) => "MatchPhase1b"
        case Request.MatchPhase2B(_) => "MatchPhase2b"
        case Request.MatchChosen(_)  => "MatchChosen"
        case Request.MatchNack(_)    => "MatchNack"
        case Request.Empty =>
          logger.fatal("Empty ReconfigurerInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Reconfigure(r)  => handleReconfigure(src, r)
        case Request.StopAck(r)      => handleStopAck(src, r)
        case Request.BootstrapAck(r) => handleBootstrapAck(src, r)
        case Request.MatchPhase1B(r) => handleMatchPhase1b(src, r)
        case Request.MatchPhase2B(r) => handleMatchPhase2b(src, r)
        case Request.MatchChosen(r)  => handleMatchChosen(src, r)
        case Request.MatchNack(r)    => handleMatchNack(src, r)
        case Request.Empty =>
          logger.fatal("Empty ReconfigurerInbound encountered.")
      }
    }
  }

  private def handleReconfigure(
      src: Transport#Address,
      reconfigure: Reconfigure
  ): Unit = {
    state match {
      case idle: Idle =>
        val leader = chan[Leader[Transport]](src, Leader.serializer)
        if (reconfigure.matchmakerConfiguration.epoch <
              idle.configuration.epoch) {
          // The reconfigure is stale. We have a more recent configuration.
          leader.send(
            LeaderInbound()
              .withMatchChosen(MatchChosen(value = idle.configuration))
          )
          return
        }

        // Sends stops to the matchmakers.
        val stop = Stop(reconfigure.matchmakerConfiguration)
        for (index <- reconfigure.matchmakerConfiguration.matchmakerIndex) {
          matchmakers(index).send(MatchmakerInbound().withStop(stop))
        }

        // Update our state.
        state = Stopping(
          configuration = reconfigure.matchmakerConfiguration,
          newConfiguration = MatchmakerConfiguration(
            epoch = reconfigure.matchmakerConfiguration.epoch + 1,
            reconfigurerIndex = index,
            matchmakerIndex = reconfigure.newMatchmakerIndex
          ),
          stopAcks = mutable.Map(),
          resendStops = makeResendStopsTimer(
            stop,
            reconfigure.matchmakerConfiguration.matchmakerIndex.toSet
          )
        )

      case _: Stopping | _: Bootstrapping | _: Phase1 | _: Phase2 =>
        logger.debug(
          s"Reconfigurer received a Reconfigure command while it is " +
            s"reconfiguring. Its state is $state. The Reconfigure command is " +
            s"being ignored."
        )
    }
  }

  private def handleStopAck(src: Transport#Address, stopAck: StopAck): Unit = {
    state match {
      case _: Idle | _: Bootstrapping | _: Phase1 | _: Phase2 =>
        logger.debug(
          s"Reconfigurer received a StopAck command but its state is " +
            s"$state. The StopAck command is being ignored."
        )

      case stopping: Stopping =>
        // Ignore incorrect epochs.
        if (stopAck.epoch != stopping.configuration.epoch) {
          return
        }

        // Wait until we have a quorum of responses.
        stopping.stopAcks(stopAck.matchmakerIndex) = stopAck
        if (stopping.stopAcks.size < config.f + 1) {
          return
        }

        // Stop our timer.
        stopping.resendStops.stop()

        // Union logs, trim garbage, and send to new matchmakers.
        val gcWatermark = stopping.stopAcks.values.map(_.gcWatermark).max
        val configurations =
          stopping.stopAcks.values
            .flatMap(_.configuration)
            .toSet
            .filter(_.round >= gcWatermark)
            .toSeq
        val bootstrap = Bootstrap(
          epoch = stopping.newConfiguration.epoch,
          reconfigurerIndex = index,
          gcWatermark = gcWatermark,
          configuration = configurations
        )
        for (index <- stopping.newConfiguration.matchmakerIndex) {
          matchmakers(index).send(
            MatchmakerInbound().withBootstrap(bootstrap)
          )
        }

        // Update our state.
        state = Bootstrapping(
          configuration = stopping.configuration,
          newConfiguration = stopping.newConfiguration,
          bootstrapAcks = mutable.Map(),
          resendBootstraps = makeResendBootstrapsTimer(
            bootstrap,
            stopping.newConfiguration.matchmakerIndex.toSet
          )
        )
    }
  }

  // TODO(mwhittaker): It is possible, though very very unlikely that one of
  // the new matchmakers fails before it can be properly bootstrapped. This
  // means that we'll be stuck waiting for it to bootstrap forever. We need a
  // timeout to trigger us to try and reconfigure to a different set of nodes.
  private def handleBootstrapAck(
      src: Transport#Address,
      bootstrapAck: BootstrapAck
  ): Unit = {
    state match {
      case _: Idle | _: Stopping | _: Phase1 | _: Phase2 =>
        logger.debug(
          s"Reconfigurer received a BootstrapAck command but its state is " +
            s"$state. The BootstrapAck command is being ignored."
        )

      case bootstrapping: Bootstrapping =>
        // Ignore incorrect epochs.
        if (bootstrapAck.epoch != bootstrapping.newConfiguration.epoch) {
          logger.debug(
            s"Reconfigurer received a BootstrapAck in epoch " +
              s"${bootstrapAck.epoch} but is already in epoch " +
              s"${bootstrapping.configuration.epoch}."
          )
          return
        }

        // Wait until we've heard back from all of them.
        bootstrapping.bootstrapAcks(bootstrapAck.matchmakerIndex) = bootstrapAck
        if (bootstrapping.bootstrapAcks.size < 2 * config.f + 1) {
          return
        }

        // Stop our timer.
        bootstrapping.resendBootstraps.stop()

        // Send Phase1as.
        val round =
          roundSystem.nextClassicRound(leaderIndex = index, round = -1)
        val matchPhase1a = MatchPhase1a(
          matchmakerConfiguration = bootstrapping.configuration,
          round = round
        )
        for (index <- bootstrapping.configuration.matchmakerIndex) {
          matchmakers(index).send(
            MatchmakerInbound().withMatchPhase1A(matchPhase1a)
          )
        }

        // Update our state.
        state = Phase1(
          configuration = bootstrapping.configuration,
          newConfiguration = bootstrapping.newConfiguration,
          round = round,
          matchPhase1bs = mutable.Map(),
          resendMatchPhase1as = makeResendMatchPhase1asTimer(
            matchPhase1a,
            bootstrapping.configuration.matchmakerIndex.toSet
          )
        )
    }
  }

  private def handleMatchPhase1b(
      src: Transport#Address,
      matchPhase1b: MatchPhase1b
  ): Unit = {
    state match {
      case _: Idle | _: Stopping | _: Bootstrapping | _: Phase2 =>
        logger.debug(
          s"Reconfigurer received a MatchPhase1b command but its state is " +
            s"$state. The MatchPhase1b command is being ignored."
        )

      case phase1: Phase1 =>
        // Ignore incorrect epochs.
        if (matchPhase1b.epoch != phase1.configuration.epoch) {
          return
        }

        // Ignore stale rounds.
        if (matchPhase1b.round != phase1.round) {
          logger.checkLt(matchPhase1b.round, phase1.round)
          return
        }

        // Wait until we've heard back from a quorum of matchmakers.
        phase1.matchPhase1bs(matchPhase1b.matchmakerIndex) = matchPhase1b
        if (phase1.matchPhase1bs.size < config.f + 1) {
          return
        }

        // Stop our timer.
        phase1.resendMatchPhase1as.stop()

        // Select safe value and propose in phase 2.
        val votes = phase1.matchPhase1bs.values.flatMap(_.vote)
        val value = if (votes.isEmpty) {
          phase1.newConfiguration
        } else {
          votes.maxBy(_.voteRound).voteValue
        }
        val matchPhase2a = MatchPhase2a(
          matchmakerConfiguration = phase1.configuration,
          round = phase1.round,
          value = value
        )
        for (index <- phase1.configuration.matchmakerIndex) {
          matchmakers(index).send(
            MatchmakerInbound().withMatchPhase2A(matchPhase2a)
          )
        }

        // Update our state.
        state = Phase2(
          configuration = phase1.configuration,
          newConfiguration = value,
          round = phase1.round,
          matchPhase2bs = mutable.Map(),
          resendMatchPhase2as = makeResendMatchPhase2asTimer(
            matchPhase2a,
            phase1.configuration.matchmakerIndex.toSet
          )
        )
    }
  }

  private def handleMatchPhase2b(
      src: Transport#Address,
      matchPhase2b: MatchPhase2b
  ): Unit = {
    state match {
      case _: Idle | _: Stopping | _: Bootstrapping | _: Phase1 =>
        logger.debug(
          s"Reconfigurer received a StopAck command but its state is " +
            s"$state. The StopAck command is being ignored."
        )

      case phase2: Phase2 =>
        // Ignore incorrect epochs.
        if (matchPhase2b.epoch != phase2.configuration.epoch) {
          return
        }

        // Ignore stale rounds.
        if (matchPhase2b.round != phase2.round) {
          logger.checkLt(matchPhase2b.round, phase2.round)
          return
        }

        // Wait until we've heard back from a quorum of matchmakers.
        phase2.matchPhase2bs(matchPhase2b.matchmakerIndex) = matchPhase2b
        if (phase2.matchPhase2bs.size < config.f + 1) {
          return
        }

        // Stop our timer.
        phase2.resendMatchPhase2as.stop()

        // Inform the matchmakers, the other reconfigurers, and the leaders.
        val matchChosen = MatchChosen(value = phase2.newConfiguration)
        leaders.foreach(_.send(LeaderInbound().withMatchChosen(matchChosen)))
        otherReconfigurers.foreach(
          _.send(ReconfigurerInbound().withMatchChosen(matchChosen))
        )
        for (index <- Set() ++ phase2.configuration.matchmakerIndex ++
               phase2.newConfiguration.matchmakerIndex) {
          matchmakers(index).send(
            MatchmakerInbound().withMatchChosen(matchChosen)
          )
        }

        // Update our state.
        state = Idle(configuration = phase2.newConfiguration)
    }
  }

  private def handleMatchChosen(
      src: Transport#Address,
      matchChosen: MatchChosen
  ): Unit = {
    // Ignore stale MatchChosens.
    val epoch = state match {
      case idle: Idle                   => idle.configuration.epoch
      case stopping: Stopping           => stopping.configuration.epoch
      case bootstrapping: Bootstrapping => bootstrapping.configuration.epoch
      case phase1: Phase1               => phase1.configuration.epoch
      case phase2: Phase2               => phase2.configuration.epoch
    }
    if (matchChosen.value.epoch <= epoch) {
      return
    }

    // Stop any running timers.
    state match {
      case idle: Idle                   =>
      case stopping: Stopping           => stopping.resendStops.stop()
      case bootstrapping: Bootstrapping => bootstrapping.resendBootstraps.stop()
      case phase1: Phase1               => phase1.resendMatchPhase1as.stop()
      case phase2: Phase2               => phase2.resendMatchPhase2as.stop()
    }

    // Update our state.
    state = Idle(matchChosen.value)
  }

  private def handleMatchNack(
      src: Transport#Address,
      nack: MatchNack
  ): Unit = {
    // MatchNacks are only relevant in Phase 1 and 2.
    val (round, epoch) = state match {
      case _: Idle | _: Stopping | _: Bootstrapping =>
        return
      case phase1: Phase1 =>
        (phase1.round, phase1.configuration.epoch)
      case phase2: Phase2 =>
        (phase2.round, phase2.configuration.epoch)
    }

    // Ignore stale requests.
    if (nack.epoch != epoch) {
      logger.debug(
        s"Leader received a nack in epoch ${nack.epoch} but is " +
          s"already in epoch $epoch. The nack is being ignored."
      )
      return
    }
    if (nack.round <= round) {
      logger.debug(
        s"Leader received a nack in round ${nack.round} but is " +
          s"already in round $round. The nack is being ignored."
      )
      return
    }

    // Re-enter Phase 1, but in a larger round.
    // TODO(mwhittaker): We should have sleeps here to avoid dueling.
    val (configuration, newConfiguration) = state match {
      case _: Idle | _: Stopping | _: Bootstrapping =>
        logger.fatal("Unreachable.")
      case phase1: Phase1 =>
        phase1.resendMatchPhase1as.stop()
        (phase1.configuration, phase1.newConfiguration)
      case phase2: Phase2 =>
        phase2.resendMatchPhase2as.stop()
        (phase2.configuration, phase2.newConfiguration)
    }

    val newRound =
      roundSystem.nextClassicRound(leaderIndex = index, round = nack.round)
    val matchPhase1a = MatchPhase1a(
      matchmakerConfiguration = configuration,
      round = newRound
    )
    for (index <- configuration.matchmakerIndex) {
      matchmakers(index).send(
        MatchmakerInbound().withMatchPhase1A(matchPhase1a)
      )
    }

    state = Phase1(
      configuration = configuration,
      newConfiguration = newConfiguration,
      round = newRound,
      matchPhase1bs = mutable.Map(),
      resendMatchPhase1as = makeResendMatchPhase1asTimer(
        matchPhase1a,
        configuration.matchmakerIndex.toSet
      )
    )
  }

  // API ///////////////////////////////////////////////////////////////////////
  // For the JS frontend.
  def reconfigureF1(
      matchmaker1: MatchmakerIndex,
      matchmaker2: MatchmakerIndex,
      matchmaker3: MatchmakerIndex
  ): Unit = {
    reconfigure(Set(matchmaker1, matchmaker2, matchmaker3))
  }
}
