package frankenpaxos.caspaxos

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
case class LeaderOptions(
    resendPhase1asTimerPeriod: java.time.Duration,
    resendPhase2asTimerPeriod: java.time.Duration,
    minNackSleepPeriod: java.time.Duration,
    maxNackSleepPeriod: java.time.Duration,
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(1),
    resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(1),
    minNackSleepPeriod = java.time.Duration.ofMillis(100),
    maxNackSleepPeriod = java.time.Duration.ofMillis(1000),
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("caspaxos_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_leader_resend_phase1as_total")
    .help("Total number of times the leader resent phase1a messages.")
    .register()

  val resendPhase2asTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_leader_resend_phase2as_total")
    .help("Total number of times the leader resent phase2a messages.")
    .register()
}

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

  type AcceptorIndex = Int

  @JSExportAll
  sealed trait State[Transport <: frankenpaxos.Transport[Transport]]

  @JSExportAll
  case class Idle[Transport <: frankenpaxos.Transport[Transport]](
      round: Int
  ) extends State[Transport]

  @JSExportAll
  case class Phase1[Transport <: frankenpaxos.Transport[Transport]](
      clientRequests: mutable.Buffer[ClientRequest],
      round: Int,
      phase1bs: mutable.Map[AcceptorIndex, Phase1b],
      resendPhase1as: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class Phase2[Transport <: frankenpaxos.Transport[Transport]](
      clientRequests: mutable.Buffer[ClientRequest],
      round: Int,
      value: Set[Int],
      phase2bs: mutable.Map[AcceptorIndex, Phase2b],
      resendPhase2as: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class WaitingToRecover[Transport <: frankenpaxos.Transport[Transport]](
      clientRequests: mutable.Buffer[ClientRequest],
      round: Int,
      recoverTimer: Transport#Timer
  ) extends State[Transport]
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Leader._
  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and compute our index.
  logger.check(config.valid())
  logger.check(config.leaderAddresses.contains(address))
  private val index = config.leaderAddresses.indexOf(address)

  // Channels to the acceptors.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (address <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](address, Acceptor.serializer)

  // The round system used to choose rounds. For simplicity, we hard code our
  // choice of round robin.
  @JSExport
  protected var roundSystem: RoundSystem =
    new RoundSystem.ClassicRoundRobin(config.leaderAddresses.size)

  // The leader's state.
  @JSExport
  protected var state: State[Transport] = Idle(
    round = roundSystem.nextClassicRound(index, round = -1)
  )

  // Note that we don't have a client table unlike a regular Paxos leader. This
  // is because of two things:
  //
  //   (1) We've assumed that all operations are set adds. Executing a set add
  //       multiple times doesn't hurt.
  //   (2) It's not clear how to add a client table to CASPaxos anyway. In
  //       protocols like MultiPaxos, Menicus, EPaxos, replicas see a complete
  //       history of commands and can record the most recent command for every
  //       client. Here, leaders may not run every transition function, so it's
  //       not as easy to record the most recent command for every client.
  //       You'd have to embed the client table in the state itself which is a
  //       bit nasty.

  // Helpers ///////////////////////////////////////////////////////////////////
  private def toIntSetProto(xs: Set[Int]): IntSet = IntSet(value = xs.toSeq)

  private def fromIntSetProto(xs: IntSet): Set[Int] = xs.value.toSet

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
      clientRequests: mutable.Buffer[ClientRequest]
  ): Unit = {
    // Send phase1as.
    val phase1a = Phase1a(round = round)
    acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))

    // Update our state.
    stopTimers(state)
    state = Phase1(
      clientRequests = clientRequests,
      round = round,
      phase1bs = mutable.Map[AcceptorIndex, Phase1b](),
      resendPhase1as = makeResendPhase1as(phase1a)
    )
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase1as(
      phase1a: Phase1a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as",
      options.resendPhase1asTimerPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPhase2as(
      phase2a: Phase2a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase2as",
      options.resendPhase2asTimerPeriod,
      () => {
        metrics.resendPhase2asTotal.inc()
        acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeRecoverTimer(
      round: Int,
      clientRequests: mutable.Buffer[ClientRequest]
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
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request

    val label =
      inbound.request match {
        case Request.ClientRequest(_) => "ClientRequest"
        case Request.Phase1B(_)       => "Phase1b"
        case Request.Phase2B(_)       => "Phase2b"
        case Request.Nack(_)          => "Nack"
        case Request.Empty => {
          logger.fatal("Empty LeaderInbound encountered.")
        }
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r) => handleClientRequest(src, r)
        case Request.Phase1B(r)       => handlePhase1b(src, r)
        case Request.Phase2B(r)       => handlePhase2b(src, r)
        case Request.Nack(r)          => handleNack(src, r)
        case Request.Empty => {
          logger.fatal("Empty LeaderInbound encountered.")
        }
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
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

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        logger.debug(s"Leader received Phase1b while idle.")
      case phase2: Phase2[Transport] =>
        logger.debug(s"Leader received Phase1b while in phase 2.")
      case waiting: WaitingToRecover[Transport] =>
        logger.debug(s"Leader received Phase1b while waiting to recover.")
      case phase1: Phase1[Transport] =>
        if (phase1b.round != phase1.round) {
          logger.debug(
            s"Leader received Phase1b in round ${phase1.round} but is in " +
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
        val minPhase1b: Phase1b = phase1.phase1bs.values.minBy(_.voteRound)
        val previousValue: Set[Int] =
          if (minPhase1b.voteRound == -1) {
            Set[Int]()
          } else {
            fromIntSetProto(minPhase1b.voteValue.get)
          }

        // Compute the new value.
        val newValue = previousValue ++ fromIntSetProto(
          phase1.clientRequests(0).intSet
        )

        // Send Phase2as.
        val phase2a = Phase2a(
          round = phase1.round,
          value = toIntSetProto(newValue)
        )
        acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))

        // Update our state.
        phase1.resendPhase1as.stop()
        state = Phase2(
          clientRequests = phase1.clientRequests,
          round = phase1.round,
          value = newValue,
          phase2bs = mutable.Map[AcceptorIndex, Phase2b](),
          resendPhase2as = makeResendPhase2as(phase2a)
        )
    }
  }

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        logger.debug("Leader received Phase2b while idle.")
      case phase1: Phase1[Transport] =>
        logger.debug("Leader received Phase2b while in phase 1.")
      case waiting: WaitingToRecover[Transport] =>
        logger.debug(s"Leader received Phase2b while waiting to recover.")
      case phase2: Phase2[Transport] =>
        if (phase2b.round != phase2.round) {
          logger.debug(
            s"Leader received Phase2b in round ${phase2.round} but is in " +
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
        val client = chan[Client[Transport]](
          transport.addressSerializer
            .fromBytes(phase2.clientRequests(0).clientAddress.toByteArray()),
          Client.serializer
        )
        client.send(
          ClientInbound().withClientReply(
            ClientReply(
              clientId = phase2.clientRequests(0).clientId,
              value = toIntSetProto(phase2.value)
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

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    val round = getRound(state)
    if (nack.higherRound <= round) {
      logger.debug(
        s"Leader received a Nack in round ${nack.higherRound} but is already " +
          s"in round $round."
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
