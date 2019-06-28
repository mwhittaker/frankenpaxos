package frankenpaxos.unanimousbpaxos

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object AcceptorInboundSerializer extends ProtoSerializer[AcceptorInbound] {
  type A = AcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class AcceptorOptions(
    // TODO(mwhittaker): Add options.
)

@JSExportAll
object AcceptorOptions {
  // TODO(mwhittaker): Add options.
  val default = AcceptorOptions()
}

@JSExportAll
class AcceptorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()
}

@JSExportAll
object Acceptor {
  val serializer = AcceptorInboundSerializer

  type Round = Int

  @JSExportAll
  case class VoteValue(
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId]
  )

  def toProto(voteValue: VoteValue): VoteValueProto = {
    VoteValueProto(commandOrNoop = voteValue.commandOrNoop,
                   dependency = voteValue.dependencies.toSeq)
  }

  def fromProto(voteValueProto: VoteValueProto): VoteValue = {
    VoteValue(commandOrNoop = voteValueProto.commandOrNoop,
              dependencies = voteValueProto.dependency.toSet)
  }

  @JSExportAll
  case class State(
      // round is the largest round that this acceptor has heard of.
      round: Round,
      // voteRound the largest round in which this acceptor has voted, or -1 if
      // the acceptor has not yet voted.
      voteRound: Round,
      // voteValue is Some(x) if the acceptor voted for value x in voteRound or
      // None if voteRound is -1.
      voteValue: Option[VoteValue]
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
  import Acceptor._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = AcceptorInbound
  override def serializer = Acceptor.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and get our index.
  logger.check(config.valid())
  logger.check(config.acceptorAddresses.contains(address))
  private val index = config.acceptorAddresses.indexOf(address)

  // Leader channels.
  val leaders: Seq[Chan[Leader[Transport]]] = for (a <- config.leaderAddresses)
    yield chan[Leader[Transport]](a, Leader.serializer)

  // The state for each vertex.
  val states = mutable.Map[VertexId, State]()

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: AcceptorInbound
  ): Unit = {
    import AcceptorInbound.Request
    inbound.request match {
      case Request.FastProposal(r) => handleFastProposal(src, r)
      case Request.Phase1A(r)      => handlePhase1a(src, r)
      case Request.Phase2A(r)      => handlePhase2a(src, r)
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
  }

  private def handleFastProposal(
      src: Transport#Address,
      fastProposal: FastProposal
  ): Unit = {
    metrics.requestsTotal.labels("FastProposal").inc()

    states.get(fastProposal.vertexId) match {
      case None =>
        // Typically with Fast Paxos, we have to receive an "any" command from
        // a proposer before we can process a client request. Here, we perform
        // an optimization and use an implicit any on round 0.
        states(fastProposal.vertexId) = State(
          round = 0,
          voteRound = 0,
          voteValue = Some(fromProto(fastProposal.value))
        )

        leaders(fastProposal.vertexId.leaderIndex).send(
          LeaderInbound().withPhase2BFast(
            Phase2bFast(
              vertexId = fastProposal.vertexId,
              acceptorId = index,
              voteValue = fastProposal.value
            )
          )
        )

      case Some(state) =>
        if (state.round == 0) {
          logger.checkEq(state.voteRound, 0)
          logger.debug(
            s"Acceptor received a FastProposal for vertex " +
              s"${fastProposal.vertexId} and is in round 0 and voted in " +
              s"round 0. We're ignoring the request since we've already voted."
          )
        } else {
          // We've seen a higher round. We should send a nack.
          leaders(fastProposal.vertexId.leaderIndex).send(
            LeaderInbound().withNack(
              Nack(
                vertexId = fastProposal.vertexId,
                higherRound = state.round
              )
            )
          )
        }
    }
  }

  private def handlePhase1a(
      src: Transport#Address,
      phase1a: Phase1a
  ): Unit = {
    metrics.requestsTotal.labels("Phase1a").inc()

    val state = states.getOrElse(
      phase1a.vertexId,
      State(round = -1, voteRound = -1, voteValue = None)
    )

    // Ignore messages from previous rounds. Note that we have < instead of <=
    // here. This is critical for liveness. If the leader re-sends its
    // Phase1a message to us, we want to re-send our reply.
    val leader = chan[Leader[Transport]](src, Leader.serializer)
    if (phase1a.round < state.round) {
      logger.debug(
        s"An acceptor received a phase 1a message in ${phase1a.vertexId} for " +
          s"round ${phase1a.round} and is in round ${state.round}."
      )
      leader.send(
        LeaderInbound().withNack(
          Nack(vertexId = phase1a.vertexId, higherRound = state.round)
        )
      )
      return
    }

    // Bump our round and send the proposer our vote round and vote value.
    states(phase1a.vertexId) = state.copy(round = phase1a.round)
    leader.send(
      LeaderInbound().withPhase1B(
        Phase1b(
          vertexId = phase1a.vertexId,
          acceptorId = index,
          round = phase1a.round,
          voteRound = state.voteRound,
          voteValue = state.voteValue.map(toProto)
        )
      )
    )
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    metrics.requestsTotal.labels("Phase2a").inc()

    val state = states.getOrElse(
      phase2a.vertexId,
      State(round = -1, voteRound = -1, voteValue = None)
    )

    // Ignore messages from previous rounds. Note that we have < instead of <=
    // here. This is typical for phase 2, since an acceptor will often vote for
    // a value in the same round that it heard during phase 1. But, this is
    // critical for liveness in another way as well. If the proposer re-sends
    // its Phase2a message to us, we want to re-send our reply.
    val leader = chan[Leader[Transport]](src, Leader.serializer)
    if (phase2a.round < state.round) {
      logger.debug(
        s"An acceptor received a phase 2a message in ${phase2a.vertexId} for " +
          s"round ${phase2a.round} but is in round ${state.round}."
      )
      leader.send(
        LeaderInbound().withNack(
          Nack(vertexId = phase2a.vertexId, higherRound = state.round)
        )
      )
      return
    }

    // Update our state and send back an ack to the proposer.
    states(phase2a.vertexId) = state.copy(
      round = phase2a.round,
      voteRound = phase2a.round,
      voteValue = Some(fromProto(phase2a.voteValue))
    )
    leader.send(
      LeaderInbound().withPhase2BClassic(
        Phase2bClassic(vertexId = phase2a.vertexId,
                       acceptorId = index,
                       round = phase2a.round)
      )
    )
  }
}
