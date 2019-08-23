package frankenpaxos.simplegcbpaxos

import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
case class CasLeaderOptions(
    resendPhase1asTimerPeriod: java.time.Duration,
    resendPhase2asTimerPeriod: java.time.Duration,
    minNackSleepPeriod: java.time.Duration,
    maxNackSleepPeriod: java.time.Duration,
    measureLatencies: Boolean
)

@JSExportAll
object CasLeaderOptions {
  val default = CasLeaderOptions(
    resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(1),
    resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(1),
    minNackSleepPeriod = java.time.Duration.ofMillis(100),
    maxNackSleepPeriod = java.time.Duration.ofMillis(1000),
    measureLatencies = true
  )
}

@JSExportAll
class CasLeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_cas_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_leader_resend_phase1as_total")
    .help("Total number of times the leader resent phase1a messages.")
    .register()

  val resendPhase2asTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_leader_resend_phase2as_total")
    .help("Total number of times the leader resent phase2a messages.")
    .register()
}

@JSExportAll
object CasLeaderInboundSerializer extends ProtoSerializer[CasLeaderInbound] {
  type A = CasLeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object CasLeader {
  val serializer = CasLeaderInboundSerializer

  type AcceptorIndex = Int

  @JSExportAll
  sealed trait State[Transport <: frankenpaxos.Transport[Transport]]

  @JSExportAll
  case class Idle[Transport <: frankenpaxos.Transport[Transport]](
      round: Int
  ) extends State[Transport]

  @JSExportAll
  case class Phase1[Transport <: frankenpaxos.Transport[Transport]](
      clientRequests: mutable.Buffer[CasClientRequest],
      round: Int,
      phase1bs: mutable.Map[AcceptorIndex, CasPhase1b],
      resendPhase1as: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class Phase2[Transport <: frankenpaxos.Transport[Transport]](
      clientRequests: mutable.Buffer[CasClientRequest],
      round: Int,
      watermark: VertexIdPrefixSet,
      phase2bs: mutable.Map[AcceptorIndex, CasPhase2b],
      resendPhase2as: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class WaitingToRecover[Transport <: frankenpaxos.Transport[Transport]](
      clientRequests: mutable.Buffer[CasClientRequest],
      round: Int,
      recoverTimer: Transport#Timer
  ) extends State[Transport]
}

@JSExportAll
class CasLeader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: CasLeaderOptions = CasLeaderOptions.default,
    metrics: CasLeaderMetrics = new CasLeaderMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import CasLeader._
  override type InboundMessage = CasLeaderInbound
  override def serializer = CasLeader.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and compute our index.
  logger.check(config.valid())
  logger.check(config.casLeaderAddresses.contains(address))
  private val index = config.casLeaderAddresses.indexOf(address)

  // Channels to the acceptors.
  private val acceptors: Seq[Chan[CasAcceptor[Transport]]] =
    for (address <- config.casAcceptorAddresses)
      yield chan[CasAcceptor[Transport]](address, CasAcceptor.serializer)

  // The round system used to choose rounds. For simplicity, we hard code our
  // choice of round robin.
  @JSExport
  protected var roundSystem: RoundSystem =
    new RoundSystem.ClassicRoundRobin(config.casLeaderAddresses.size)

  // The leader's state.
  @JSExport
  protected var state: State[Transport] = Idle(
    round = roundSystem.nextClassicRound(index, round = -1)
  )

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

  private def getRound(state: State[Transport]): Int = {
    state match {
      case idle: Idle[Transport]                => idle.round
      case phase1: Phase1[Transport]            => phase1.round
      case phase2: Phase2[Transport]            => phase2.round
      case waiting: WaitingToRecover[Transport] => waiting.round
    }
  }

  private def stopTimers(state: State[Transport]): Unit = {
    state match {
      case idle: Idle[Transport]                =>
      case phase1: Phase1[Transport]            => phase1.resendPhase1as.stop()
      case phase2: Phase2[Transport]            => phase2.resendPhase2as.stop()
      case waiting: WaitingToRecover[Transport] => waiting.recoverTimer.stop()
    }
  }

  private def transitionToPhase1(
      round: Int,
      clientRequests: mutable.Buffer[CasClientRequest]
  ): Unit = {
    // Send phase1as.
    val phase1a = CasPhase1a(round = round)
    acceptors.foreach(_.send(CasAcceptorInbound().withCasPhase1A(phase1a)))

    // Update our state.
    stopTimers(state)
    state = Phase1(
      clientRequests = clientRequests,
      round = round,
      phase1bs = mutable.Map[AcceptorIndex, CasPhase1b](),
      resendPhase1as = makeResendPhase1as(phase1a)
    )
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase1as(
      phase1a: CasPhase1a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as",
      options.resendPhase1asTimerPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        acceptors.foreach(_.send(CasAcceptorInbound().withCasPhase1A(phase1a)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPhase2as(
      phase2a: CasPhase2a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase2as",
      options.resendPhase2asTimerPeriod,
      () => {
        metrics.resendPhase2asTotal.inc()
        acceptors.foreach(_.send(CasAcceptorInbound().withCasPhase2A(phase2a)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeRecoverTimer(
      round: Int,
      clientRequests: mutable.Buffer[CasClientRequest]
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"recover",
      frankenpaxos.Util.randomDuration(options.minNackSleepPeriod,
                                       options.maxNackSleepPeriod),
      () => transitionToPhase1(round, clientRequests)
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: CasLeaderInbound
  ): Unit = {
    import CasLeaderInbound.Request

    val label =
      inbound.request match {
        case Request.CasClientRequest(_) => "CasClientRequest"
        case Request.CasPhase1B(_)       => "CasPhase1b"
        case Request.CasPhase2B(_)       => "CasPhase2b"
        case Request.CasNack(_)          => "CasNack"
        case Request.Empty => {
          logger.fatal("Empty CasLeaderInbound encountered.")
        }
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.CasClientRequest(r) => handleCasClientRequest(src, r)
        case Request.CasPhase1B(r)       => handleCasPhase1b(src, r)
        case Request.CasPhase2B(r)       => handleCasPhase2b(src, r)
        case Request.CasNack(r)          => handleCasNack(src, r)
        case Request.Empty => {
          logger.fatal("Empty CasLeaderInbound encountered.")
        }
      }
    }
  }

  private def handleCasClientRequest(
      src: Transport#Address,
      clientRequest: CasClientRequest
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        transitionToPhase1(idle.round, mutable.Buffer(clientRequest))
      case phase1: Phase1[Transport] =>
        // Buffer the client request for later.
        phase1.clientRequests += clientRequest
      case phase2: Phase2[Transport] =>
        // Buffer the client request for later.
        phase2.clientRequests += clientRequest
      case waiting: WaitingToRecover[Transport] =>
        // Buffer the client request for later.
        waiting.clientRequests += clientRequest
    }
  }

  private def handleCasPhase1b(
      src: Transport#Address,
      phase1b: CasPhase1b
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        logger.debug(s"CasLeader received Phase1b while idle.")
      case phase2: Phase2[Transport] =>
        logger.debug(s"CasLeader received Phase1b while in phase 2.")
      case waiting: WaitingToRecover[Transport] =>
        logger.debug(s"CasLeader received Phase1b while waiting to recover.")
      case phase1: Phase1[Transport] =>
        if (phase1b.round != phase1.round) {
          logger.debug(
            s"CasLeader received Phase1b in round ${phase1.round} but is in " +
              s"round ${phase1.round}."
          )
          // If phase1b.round were larger, then we'd have received a Nack
          // instead of a Phase1b.
          logger.checkLt(phase1b.round, phase1.round)
          return
        }

        // Wait until we have a quorum of Phase1bs.
        phase1.phase1bs(phase1b.acceptorIndex) = phase1b
        if (phase1.phase1bs.size < config.quorumSize) {
          return
        }

        // Compute the largest vote round k. If k == -1, then no value has yet
        // been chosen, so we go with an initial value of {}. Otherwise, we use
        // the vote value in round k.
        val minPhase1b: CasPhase1b = phase1.phase1bs.values.minBy(_.voteRound)
        val watermark: VertexIdPrefixSet =
          if (minPhase1b.voteRound == -1) {
            VertexIdPrefixSet(config.leaderAddresses.size)
          } else {
            VertexIdPrefixSet.fromProto(minPhase1b.voteValue.get)
          }

        // Compute the new value.
        watermark.addAll(
          VertexIdPrefixSet.fromProto(phase1.clientRequests(0).watermark)
        )

        // Send Phase2as.
        val phase2a = CasPhase2a(
          round = phase1.round,
          watermark = watermark.toProto()
        )
        acceptors.foreach(_.send(CasAcceptorInbound().withCasPhase2A(phase2a)))

        // Update our state.
        phase1.resendPhase1as.stop()
        state = Phase2(
          clientRequests = phase1.clientRequests,
          round = phase1.round,
          watermark = watermark,
          phase2bs = mutable.Map[AcceptorIndex, CasPhase2b](),
          resendPhase2as = makeResendPhase2as(phase2a)
        )
    }
  }

  private def handleCasPhase2b(
      src: Transport#Address,
      phase2b: CasPhase2b
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        logger.debug("CasLeader received Phase2b while idle.")
      case phase1: Phase1[Transport] =>
        logger.debug("CasLeader received Phase2b while in phase 1.")
      case waiting: WaitingToRecover[Transport] =>
        logger.debug(s"CasLeader received Phase2b while waiting to recover.")
      case phase2: Phase2[Transport] =>
        if (phase2b.round != phase2.round) {
          logger.debug(
            s"CasLeader received Phase2b in round ${phase2.round} but is in " +
              s"round ${phase2.round}."
          )
          // If phase2b.round were larger, then we'd have received a Nack
          // instead of a Phase2b.
          logger.checkLt(phase2b.round, phase2.round)
          return
        }

        // Wait until we have a quorum of Phase2bs.
        phase2.phase2bs(phase2b.acceptorIndex) = phase2b
        if (phase2.phase2bs.size < config.quorumSize) {
          return
        }

        // Reply to the client.
        val client = chan[CasClient[Transport]](
          transport.addressSerializer
            .fromBytes(phase2.clientRequests(0).clientAddress.toByteArray()),
          CasClient.serializer
        )
        client.send(
          CasClientInbound().withCasClientReply(
            CasClientReply(
              clientId = phase2.clientRequests(0).clientId,
              watermark = phase2.watermark.toProto()
            )
          )
        )

        // Update our state.
        phase2.resendPhase2as.stop()
        val round = roundSystem.nextClassicRound(index, phase2.round)
        if (phase2.clientRequests.size == 1) {
          state = Idle(round = round)
        } else {
          phase2.clientRequests.remove(0)
          transitionToPhase1(round, phase2.clientRequests)
        }
    }
  }

  private def handleCasNack(
      src: Transport#Address,
      nack: CasNack
  ): Unit = {
    val round = getRound(state)
    if (nack.higherRound <= round) {
      logger.debug(
        s"CasLeader received a Nack in round ${nack.higherRound} but is " +
          s"already in round $round."
      )
      return
    }

    val newRound = roundSystem.nextClassicRound(index, nack.higherRound)
    stopTimers(state)
    state match {
      // If we're idle, then we ignore nacks.
      case idle: Idle[Transport] =>
        state = Idle(round = newRound)
      // In all other cases, we wait to recover to avoid dueling leaders.
      case phase1: Phase1[Transport] =>
        state = WaitingToRecover(
          clientRequests = phase1.clientRequests,
          round = newRound,
          recoverTimer = makeRecoverTimer(newRound, phase1.clientRequests)
        )
      case phase2: Phase2[Transport] =>
        state = WaitingToRecover(
          clientRequests = phase2.clientRequests,
          round = newRound,
          recoverTimer = makeRecoverTimer(newRound, phase2.clientRequests)
        )
      case waiting: WaitingToRecover[Transport] =>
        state = WaitingToRecover(
          clientRequests = waiting.clientRequests,
          round = newRound,
          recoverTimer = makeRecoverTimer(newRound, waiting.clientRequests)
        )
    }
  }
}
