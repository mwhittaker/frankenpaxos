package frankenpaxos.simplebpaxos

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.util
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
    // The `growSize` of the Acceptors' underlying BufferMaps.
    // DO_NOT_SUBMIT(mwhittaker): Add flags to executables and Python.
    statesGrowSize: Int
)

@JSExportAll
object AcceptorOptions {
  val default = AcceptorOptions(
    statesGrowSize = 5000
  )
}

@JSExportAll
class AcceptorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_bpaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_bpaxos_acceptor_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
object Acceptor {
  val serializer = AcceptorInboundSerializer

  type Round = Int

  @JSExportAll
  case class VoteValue(
      commandOrNoop: CommandOrNoop,
      dependencies: VertexIdPrefixSet
  )

  def toProto(voteValue: VoteValue): VoteValueProto = {
    VoteValueProto(commandOrNoop = voteValue.commandOrNoop,
                   dependencies = voteValue.dependencies.toProto)
  }

  def fromProto(voteValueProto: VoteValueProto): VoteValue = {
    VoteValue(commandOrNoop = voteValueProto.commandOrNoop,
              dependencies =
                VertexIdPrefixSet.fromProto(voteValueProto.dependencies))
  }

  @JSExportAll
  case class State(
      round: Round,
      voteRound: Round,
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

  // The state of every vertex.
  val states = new VertexIdBufferMap[State](
    numLeaders = config.leaderAddresses.size,
    growSize = options.statesGrowSize
  )

  // The garbage collection watermark. If n is the number of leaders, then
  // gcQuorumWatermarkVector.watermark() is a vector of length n. Say the ith
  // entry of the watermark is j. Then all vertices with leader index i and id
  // less than j can be garbage collected.
  @JSExport
  protected val gcQuorumWatermarkVector = new util.QuorumWatermarkVector(
    n = config.replicaAddresses.size,
    depth = config.leaderAddresses.size
  )

  // gcWatermark is the cached value of gcQuorumWatermarkVector.watermark(). We
  // cache it because we refer to it often, but it's ok if its stale.
  @JSExport
  protected var gcWatermark: Seq[Int] =
    gcQuorumWatermarkVector.watermark(quorumSize = config.f + 1)

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: AcceptorInbound
  ): Unit = {
    import AcceptorInbound.Request

    val startNanos = System.nanoTime
    val label = inbound.request match {
      case Request.Phase1A(r) =>
        handlePhase1a(src, r)
        "Phase1A"
      case Request.Phase2A(r) =>
        handlePhase2a(src, r)
        "Phase2A"
      case Request.GarbageCollect(r) =>
        handleGarbageCollect(src, r)
        "GarbageCollect"
      case Request.Empty => {
        logger.fatal("Empty AcceptorInbound encountered.")
      }
    }
    val stopNanos = System.nanoTime
    metrics.requestsTotal.labels(label).inc()
    metrics.requestsLatency
      .labels(label)
      .observe((stopNanos - startNanos).toDouble / 1000000)
  }

  private def handlePhase1a(
      src: Transport#Address,
      phase1a: Phase1a
  ): Unit = {
    // Ignore garbage collected vertices.
    //
    // TODO(mwhittaker): This is not super live. If an acceptor garbage
    // collects something but a proposer does not, it's possible the proposer
    // will keep re-sending to the acceptors. In the normal case, the proposers
    // and acceptors will garbage collect the same things, so it should be
    // fine. If we wanted to be more thorough, we could have acceptors return
    // the watermark it knows about to the proposer.
    if (phase1a.vertexId.id < gcWatermark(phase1a.vertexId.leaderIndex)) {
      logger.debug(
        s"Acceptor received a Phase1a message for vertex ${phase1a.vertexId} " +
          s"but has a watermark of $gcWatermark, so the vertex has already " +
          s"been garbage collected. The message is being ignored."
      )
      return
    }

    val state = states
      .get(phase1a.vertexId)
      .getOrElse(State(round = -1, voteRound = -1, voteValue = None))

    // Ignore messages from previous rounds. Note that we have < instead of <=
    // here. This is critical for liveness. If the proposer re-sends its
    // Phase1a message to us, we want to re-send our reply.
    val proposer = chan[Proposer[Transport]](src, Proposer.serializer)
    if (phase1a.round < state.round) {
      logger.debug(
        s"An acceptor received a phase 1a message in ${phase1a.vertexId} for " +
          s"round ${phase1a.round} but is in round ${state.round}."
      )
      proposer.send(
        ProposerInbound().withNack(
          Nack(vertexId = phase1a.vertexId, higherRound = state.round)
        )
      )
      return
    }

    // Bump our round and send the proposer our vote round and vote value.
    states.put(phase1a.vertexId, state.copy(round = phase1a.round))
    proposer.send(
      ProposerInbound().withPhase1B(
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
    // Ignore garbage collected vertices.
    //
    // TODO(mwhittaker): See the note above on liveness.
    if (phase2a.vertexId.id < gcWatermark(phase2a.vertexId.leaderIndex)) {
      logger.debug(
        s"Acceptor received a Phase2a message for vertex ${phase2a.vertexId} " +
          s"but has a watermark of $gcWatermark, so the vertex has already " +
          s"been garbage collected. The message is being ignored."
      )
      return
    }

    val state = states
      .get(phase2a.vertexId)
      .getOrElse(State(round = -1, voteRound = -1, voteValue = None))

    // Ignore messages from previous rounds. Note that we have < instead of <=
    // here. This is typical for phase 2, since an acceptor will often vote for
    // a value in the same round that it heard during phase 1. But, this is
    // critical for liveness in another way as well. If the proposer re-sends
    // its Phase2a message to us, we want to re-send our reply.
    val proposer = chan[Proposer[Transport]](src, Proposer.serializer)
    if (phase2a.round < state.round) {
      logger.debug(
        s"An acceptor received a phase 2a message in ${phase2a.vertexId} for " +
          s"round ${phase2a.round} but is in round ${state.round}."
      )
      proposer.send(
        ProposerInbound().withNack(
          Nack(vertexId = phase2a.vertexId, higherRound = state.round)
        )
      )
      return
    }

    // Update our state and send back an ack to the proposer.
    states.put(phase2a.vertexId,
               state.copy(
                 round = phase2a.round,
                 voteRound = phase2a.round,
                 voteValue = Some(fromProto(phase2a.voteValue))
               ))
    proposer.send(
      ProposerInbound().withPhase2B(
        Phase2b(vertexId = phase2a.vertexId,
                acceptorId = index,
                round = phase2a.round)
      )
    )
  }

  private def handleGarbageCollect(
      src: Transport#Address,
      garbageCollect: GarbageCollect
  ): Unit = {
    // Update the GC watermark.
    gcQuorumWatermarkVector.update(
      garbageCollect.replicaIndex,
      garbageCollect.frontier
    )
    gcWatermark = gcQuorumWatermarkVector.watermark(quorumSize = config.f + 1)

    // Garbage collect all entries lower than the watermark.
    states.garbageCollect(gcWatermark)
  }
}
