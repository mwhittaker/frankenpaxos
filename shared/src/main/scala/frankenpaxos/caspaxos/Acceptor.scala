package frankenpaxos.caspaxos

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
case class AcceptorOptions(
    measureLatencies: Boolean
)

@JSExportAll
object AcceptorOptions {
  val default = AcceptorOptions(
    measureLatencies = true
  )
}

@JSExportAll
class AcceptorMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_acceptor_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("caspaxos_acceptor_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
object AcceptorInboundSerializer extends ProtoSerializer[AcceptorInbound] {
  type A = AcceptorInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Acceptor {
  val serializer = AcceptorInboundSerializer
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
  override type InboundMessage = AcceptorInbound
  override def serializer = Acceptor.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and compute our index.
  logger.check(config.valid())
  logger.check(config.acceptorAddresses.contains(address))
  private val index = config.acceptorAddresses.indexOf(address)

  // The largest round this acceptor has ever heard of.
  @JSExport
  protected var round: Int = -1

  // The largest round this acceptor has voted in.
  @JSExport
  protected var voteRound: Int = -1

  // The value voted for in voteRound. In this simple implementation of
  // CASPaxos, the state is a set of integers.
  @JSExport
  protected var voteValue: Option[Set[Int]] = None

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

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: AcceptorInbound
  ): Unit = {
    import AcceptorInbound.Request

    val label =
      inbound.request match {
        case Request.Phase1A(_) => "Phase1a"
        case Request.Phase2A(_) => "Phase2a"
        case Request.Empty => {
          logger.fatal("Empty AcceptorInbound encountered.")
        }
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase1A(r) => handlePhase1a(src, r)
        case Request.Phase2A(r) => handlePhase2a(src, r)
        case Request.Empty => {
          logger.fatal("Empty AcceptorInbound encountered.")
        }
      }
    }
  }

  private def handlePhase1a(
      src: Transport#Address,
      phase1a: Phase1a
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // Nack messages from earlier rounds.
    if (phase1a.round < round) {
      logger.debug(
        s"Acceptor received a Phase1a in round ${phase1a.round} but is " +
          s"already in round $round. The acceptor is sending back a nack."
      )
      leader.send(LeaderInbound().withNack(Nack(higherRound = round)))
    }

    round = phase1a.round
    leader.send(
      LeaderInbound().withPhase1B(
        Phase1b(round = round,
                acceptorIndex = index,
                voteRound = voteRound,
                voteValue = voteValue.map(toIntSetProto))
      )
    )
  }

  private def handlePhase2a(
      src: Transport#Address,
      phase2a: Phase2a
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // Nack messages from earlier rounds.
    if (phase2a.round < round) {
      logger.debug(
        s"Acceptor received a Phase2a in round ${phase2a.round} but is " +
          s"already in round $round. The acceptor is sending back a nack."
      )
      leader.send(LeaderInbound().withNack(Nack(higherRound = round)))
    }

    round = round
    voteRound = round
    voteValue = Some(fromIntSetProto(phase2a.value))
    leader.send(
      LeaderInbound().withPhase2B(Phase2b(round = round, acceptorIndex = index))
    )
  }
}
