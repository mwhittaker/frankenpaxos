package frankenpaxos.simplegcbpaxos

import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
case class CasAcceptorOptions(
    measureLatencies: Boolean
)

@JSExportAll
object CasAcceptorOptions {
  val default = CasAcceptorOptions(
    measureLatencies = true
  )
}

@JSExportAll
class CasAcceptorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_cas_acceptor_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
object CasAcceptorInboundSerializer
    extends ProtoSerializer[CasAcceptorInbound] {
  type A = CasAcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object CasAcceptor {
  val serializer = CasAcceptorInboundSerializer
}

@JSExportAll
class CasAcceptor[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: CasAcceptorOptions = CasAcceptorOptions.default,
    metrics: CasAcceptorMetrics = new CasAcceptorMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  override type InboundMessage = CasAcceptorInbound
  override def serializer = CasAcceptor.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and compute our index.
  logger.check(config.valid())
  logger.check(config.casAcceptorAddresses.contains(address))
  private val index = config.casAcceptorAddresses.indexOf(address)

  // The largest round this acceptor has ever heard of.
  @JSExport
  protected var round: Int = -1

  // The largest round this acceptor has voted in.
  @JSExport
  protected var voteRound: Int = -1

  // The value voted for in voteRound.
  @JSExport
  protected var voteValue: Option[VertexIdPrefixSet] = None

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
  override def receive(
      src: Transport#Address,
      inbound: CasAcceptorInbound
  ): Unit = {
    import CasAcceptorInbound.Request

    val label =
      inbound.request match {
        case Request.CasPhase1A(_) => "CasPhase1a"
        case Request.CasPhase2A(_) => "CasPhase2a"
        case Request.Empty => {
          logger.fatal("Empty CasAcceptorInbound encountered.")
        }
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.CasPhase1A(r) => handleCasPhase1a(src, r)
        case Request.CasPhase2A(r) => handleCasPhase2a(src, r)
        case Request.Empty => {
          logger.fatal("Empty CasAcceptorInbound encountered.")
        }
      }
    }
  }

  private def handleCasPhase1a(
      src: Transport#Address,
      phase1a: CasPhase1a
  ): Unit = {
    val leader = chan[CasLeader[Transport]](src, CasLeader.serializer)

    // Nack messages from earlier rounds.
    if (phase1a.round < round) {
      logger.debug(
        s"CasAcceptor received a Phase1a in round ${phase1a.round} but is " +
          s"already in round $round. The acceptor is sending back a nack."
      )
      leader.send(CasLeaderInbound().withCasNack(CasNack(higherRound = round)))
      return
    }

    round = phase1a.round
    leader.send(
      CasLeaderInbound().withCasPhase1B(
        CasPhase1b(round = round,
                   acceptorIndex = index,
                   voteRound = voteRound,
                   voteValue = voteValue.map(_.toProto()))
      )
    )
  }

  private def handleCasPhase2a(
      src: Transport#Address,
      phase2a: CasPhase2a
  ): Unit = {
    val leader = chan[CasLeader[Transport]](src, CasLeader.serializer)

    // Nack messages from earlier rounds.
    if (phase2a.round < round) {
      logger.debug(
        s"CasAcceptor received a Phase2a in round ${phase2a.round} but is " +
          s"already in round $round. The acceptor is sending back a nack."
      )
      leader.send(CasLeaderInbound().withCasNack(CasNack(higherRound = round)))
      return
    }

    round = round
    voteRound = round
    voteValue = Some(VertexIdPrefixSet.fromProto(phase2a.watermark))
    leader.send(
      CasLeaderInbound().withCasPhase2B(
        CasPhase2b(round = round, acceptorIndex = index)
      )
    )
  }
}
