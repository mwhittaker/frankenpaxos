package frankenpaxos.simplebpaxos

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.roundsystem.RoundSystem
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._

@JSExportAll
object ProposerInboundSerializer extends ProtoSerializer[ProposerInbound] {
  type A = ProposerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class ProposerOptions(
    // TODO(mwhittaker): Add options.
    resendPhase1asTimerPeriod: java.time.Duration,
    resendPhase2asTimerPeriod: java.time.Duration
)

@JSExportAll
object ProposerOptions {
  // TODO(mwhittaker): Add options.
  val default = ProposerOptions(
    resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(1),
    resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(1)
  )
}

@JSExportAll
class ProposerMetrics(collectors: Collectors) {
  // TODO(mwhittaker): Add metrics.
}

@JSExportAll
object Proposer {
  val serializer = ProposerInboundSerializer

  type Round = Int
  type AcceptorId = Int

  @JSExportAll
  sealed trait State[Transport <: frankenpaxos.Transport[Transport]]

  @JSExportAll
  case class Phase1[Transport <: frankenpaxos.Transport[Transport]](
      // A promise to fulfill once a value has been chosen.
      promise: Promise[Acceptor.VoteValue],
      // The current round.
      round: Round,
      // The pending value that this proposer wants to get chosen.
      value: Acceptor.VoteValue,
      // Phase 1b responses.
      phase1bs: mutable.Map[AcceptorId, Phase1b],
      // A timer to resend phase 1as.
      resendPhase1as: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class Phase2[Transport <: frankenpaxos.Transport[Transport]](
      // A promise to fulfill once a value has been chosen.
      promise: Promise[Acceptor.VoteValue],
      // The current round.
      round: Round,
      // The value that this proposer is proposing.
      value: Acceptor.VoteValue,
      // Phase 2b responses.
      phase2bs: mutable.Map[AcceptorId, Phase2b],
      // A timer to resend phase 2as.
      resendPhase2as: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class Chosen[Transport <: frankenpaxos.Transport[Transport]]()
      extends State[Transport]
}

@JSExportAll
class Proposer[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ProposerOptions = ProposerOptions.default,
    metrics: ProposerMetrics = new ProposerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Proposer._

  // Sanity check the configuration and get our index.
  logger.check(config.valid())
  logger.check(config.proposerAddresses.contains(address))
  private val index = config.proposerAddresses.indexOf(address)

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ProposerInbound
  override def serializer = Proposer.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (address <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](address, Acceptor.serializer)

  @JSExport
  protected val states = mutable.Map[VertexId, State[Transport]]()

  // Helpers ///////////////////////////////////////////////////////////////////
  def roundSystem(vertexId: VertexId): RoundSystem =
    new RoundSystem.RotatedClassicRoundRobin(config.n, vertexId.leaderIndex)

  def proposeImpl(
      vertexId: VertexId,
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId],
      promise: Promise[Acceptor.VoteValue]
  ): Unit = {
    states.get(vertexId) match {
      case Some(_) =>
        logger.fatal(
          s"Proposer received a proposal in $vertexId, but is already " +
            s"processing proposal in this vertex id. propose should be " +
            s"called at most once per vertex id."
        )

      case None =>
        val value = Acceptor.VoteValue(commandOrNoop, dependencies)
        val round = roundSystem(vertexId).nextClassicRound(index, -1)

        // If we're the leader of round 0, then we can skip phase 1 and proceed
        // directly to phase 2. Otherwise, we have to execute phase 1 before
        // phase 2.
        if (round == 0) {
          // Send phase2a to all acceptors.
          // TODO(mwhittaker): Add thriftiness.
          val phase2a = Phase2a(vertexId = vertexId,
                                round = round,
                                voteValue = Acceptor.toProto(value))
          acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))

          // Update our state.
          states(vertexId) = Phase2(
            promise = promise,
            round = round,
            value = value,
            phase2bs = mutable.Map[AcceptorId, Phase2b](),
            resendPhase2as = makeResendPhase2asTimer(phase2a)
          )
        } else {
          // Send phase1a to all acceptors.
          // TODO(mwhittaker): Add thriftiness.
          val phase1a = Phase1a(vertexId = vertexId, round = round)
          acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))

          // Update our state.
          states(vertexId) = Phase1(
            promise = promise,
            round = round,
            value = value,
            phase1bs = mutable.Map[AcceptorId, Phase1b](),
            resendPhase1as = makeResendPhase1asTimer(phase1a)
          )
        }
    }
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPhase1asTimer(
      phase1a: Phase1a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1a [vertexId=${phase1a.vertexId}, round=${phase1a.round}]",
      options.resendPhase1asTimerPeriod,
      () => {
        acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPhase2asTimer(
      phase2a: Phase2a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase2a [vertexId=${phase2a.vertexId}, round=${phase2a.round}]",
      options.resendPhase2asTimerPeriod,
      () => {
        acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))
        t.start()
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ProposerInbound
  ): Unit = {
    import ProposerInbound.Request
    inbound.request match {
      case Request.Phase1B(r) => handlePhase1b(src, r)
      case Request.Phase2B(r) => handlePhase2b(src, r)
      case Request.Nack(r)    => handleNack(src, r)
      case Request.Empty => {
        logger.fatal("Empty ProposerInbound encountered.")
      }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    states.get(phase1b.vertexId) match {
      case state @ (None | Some(_: Phase2[_]) | Some(_: Chosen[_])) =>
        logger.warn(
          s"Proposer received a phase1b in ${phase1b.vertexId}, but is not " +
            s"currently in phase 1 for this vertex id. The state is $state."
        )

      case Some(phase1: Phase1[Transport]) =>
        // Ignore phase1bs from old rounds.
        if (phase1b.round != phase1.round) {
          // We know that phase1b.round is less than phase1.round because if it
          // were higher, we would have received a Nack instead of a Phase1b.
          logger.check_lt(phase1b.round, phase1.round)
          logger.warn(
            s"Proposer received a phase1b in round ${phase1b.round} in " +
              s"${phase1b.vertexId} but is in round ${phase1b.round}."
          )
          return
        }

        // Wait until we have a quorum of responses.
        phase1.phase1bs(phase1b.acceptorId) = phase1b
        if (phase1.phase1bs.size < config.quorumSize) {
          return
        }

        // If we have a quorum of responses, then we can proceed. If no
        // acceptor in the quorum has voted yet (the hopefully common case),
        // then we're free to propose whatever we want. Otherwise, if at least
        // some acceptor in the quorum has voted, then we must propose the
        // value with the highest vote round.
        val maxVoteRound = phase1.phase1bs.values.map(_.voteRound).max
        val proposal: Acceptor.VoteValue = if (maxVoteRound == -1) {
          phase1.value
        } else {
          val proto = phase1.phase1bs.values
            .find(_.voteRound == maxVoteRound)
            .get
            .voteValue
            .get
          Acceptor.fromProto(proto)
        }

        // Send phase2as to the acceptors.
        // TODO(mwhittaker): Implement thriftiness.
        val phase2a = Phase2a(
          vertexId = phase1b.vertexId,
          round = phase1.round,
          voteValue = Acceptor.toProto(proposal)
        )
        acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))

        // Stop existing timers and update our state.
        phase1.resendPhase1as.stop()
        states(phase1b.vertexId) = Phase2(
          promise = phase1.promise,
          round = phase1.round,
          value = proposal,
          phase2bs = mutable.Map[AcceptorId, Phase2b](),
          resendPhase2as = makeResendPhase2asTimer(phase2a)
        )
    }
  }

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = {
    states.get(phase2b.vertexId) match {
      case state @ (None | Some(_: Phase1[_]) | Some(_: Chosen[_])) =>
        logger.warn(
          s"Proposer received a phase2b in ${phase2b.vertexId}, but is not " +
            s"currently in phase 2 for this vertex id. The state is $state."
        )

      case Some(phase2: Phase2[Transport]) =>
        // Ignore phase2bs from old rounds.
        if (phase2b.round != phase2.round) {
          // We know that phase2b.round is less than phase2.round because if it
          // were higher, we would have received a Nack instead of a Phase2b.
          logger.check_lt(phase2b.round, phase2.round)
          logger.warn(
            s"Proposer received a phase2b in round ${phase2b.round} in " +
              s"${phase2b.vertexId} but is in round ${phase2b.round}."
          )
          return
        }

        // Wait until we have a quorum of responses.
        phase2.phase2bs(phase2b.acceptorId) = phase2b
        if (phase2.phase2bs.size < config.quorumSize) {
          return
        }

        // Once we have a quorum of responses, the value is chosen!
        phase2.promise.success(phase2.value)

        // Stop existing timers and update our state.
        phase2.resendPhase2as.stop()
        states(phase2b.vertexId) = Chosen[Transport]()
    }
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    // TODO(mwhittaker): Add a random timer to avoid dueling proposers.

    val round =
      roundSystem(nack.vertexId).nextClassicRound(index, nack.higherRound)
    states.get(nack.vertexId) match {
      case None =>
        logger.warn(
          s"Proposer received a nack in ${nack.vertexId}, but is not " +
            s"currently leading ${nack.vertexId}."
        )

      case Some(chosen: Chosen[Transport]) =>
        logger.warn(
          s"Proposer received a nack in ${nack.vertexId}, but a value was " +
            s"already chosen in ${nack.vertexId}."
        )

      case Some(phase1: Phase1[Transport]) =>
        // Ignore the nack if it's stale.
        if (nack.higherRound <= phase1.round) {
          logger.warn(
            s"Proposer received a nack in ${nack.vertexId} for round " +
              s"${nack.higherRound}, but is already in round ${phase1.round}."
          )
          return
        }

        // Send phase1as to all acceptors.
        // TODO(mwhittaker): Implement thriftiness.
        val phase1a = Phase1a(vertexId = nack.vertexId, round = round)
        acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))

        // Stop existing timers and update state.
        phase1.resendPhase1as.stop()
        states(nack.vertexId) = Phase1(
          promise = phase1.promise,
          round = round,
          value = phase1.value,
          phase1bs = mutable.Map[AcceptorId, Phase1b](),
          resendPhase1as = makeResendPhase1asTimer(phase1a)
        )

      case Some(phase2: Phase2[Transport]) =>
        // Ignore the nack if it's stale.
        if (nack.higherRound <= phase2.round) {
          logger.warn(
            s"Proposer received a nack in ${nack.vertexId} for round " +
              s"${nack.higherRound}, but is already in round ${phase2.round}."
          )
          return
        }

        // Send phase1as to all acceptors.
        // TODO(mwhittaker): Implement thriftiness.
        val phase1a = Phase1a(vertexId = nack.vertexId, round = round)
        acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))

        // Stop existing timers and update state.
        phase2.resendPhase2as.stop()
        states(nack.vertexId) = Phase1(
          promise = phase2.promise,
          round = round,
          value = phase2.value,
          phase1bs = mutable.Map[AcceptorId, Phase1b](),
          resendPhase1as = makeResendPhase1asTimer(phase1a)
        )
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  private[simplebpaxos] def propose(
      vertexId: VertexId,
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId]
  ): Future[Acceptor.VoteValue] = {
    val promise = Promise[Acceptor.VoteValue]()
    proposeImpl(vertexId, commandOrNoop, dependencies, promise)
    promise.future
  }
}
