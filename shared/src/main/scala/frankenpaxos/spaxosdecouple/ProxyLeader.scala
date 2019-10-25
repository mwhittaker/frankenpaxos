package frankenpaxos.spaxosdecouple

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object ProxyLeaderInboundSerializer
    extends ProtoSerializer[ProxyLeaderInbound] {
  type A = ProxyLeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object ProxyLeader {
  val serializer = ProxyLeaderInboundSerializer
}

@JSExportAll
case class ProxyLeaderOptions(
    // A proxy leader flushes all of its channels to the acceptors after every
    // `flushPhase2asEveryN` Phase2a messages sent. For example, if
    // `flushPhase2asEveryN` is 1, then the proxy leader flushes after every
    // send.
    flushPhase2asEveryN: Int,
    measureLatencies: Boolean
)

@JSExportAll
object ProxyLeaderOptions {
  val default = ProxyLeaderOptions(
    flushPhase2asEveryN = 1,
    measureLatencies = true
  )
}

@JSExportAll
class ProxyLeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("multipaxos_proxy_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("multipaxos_proxy_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class ProxyLeader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ProxyLeaderOptions = ProxyLeaderOptions.default,
    metrics: ProxyLeaderMetrics = new ProxyLeaderMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  import ProxyLeader._
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ProxyLeaderInbound
  override val serializer = ProxyLeaderInboundSerializer

  type AcceptorIndex = Int

  @JSExportAll
  case class SlotRound(slot: Int, round: Int)

  @JSExportAll
  sealed trait State

  @JSExportAll
  case class Pending(
      phase2a: Phase2a,
      phase2bs: mutable.Map[AcceptorIndex, Phase2b]
  ) extends State

  @JSExportAll
  object Done extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // Acceptor channels.
  private val acceptors: Seq[Seq[Chan[Acceptor[Transport]]]] =
    for (acceptorCluster <- config.acceptorAddresses) yield {
      for (address <- acceptorCluster)
        yield chan[Acceptor[Transport]](address, Acceptor.serializer)
    }

  // Replica channels.
  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (address <- config.replicaAddresses)
      yield chan[Replica[Transport]](address, Replica.serializer)

  // The number of Phase2a messages since the last flush.
  private var numPhase2asSentSinceLastFlush: Int = 0

  @JSExport
  protected var states = mutable.Map[SlotRound, State]()

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
    import ProxyLeaderInbound.Request

    val label =
      inbound.request match {
        case Request.Phase2A(_) => "Phase2a"
        case Request.Phase2B(_) => "Phase2b"
        case Request.Empty =>
          logger.fatal("Empty ProxyLeaderInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.Phase2A(r) => handlePhase2a(src, r)
        case Request.Phase2B(r) => handlePhase2b(src, r)
        case Request.Empty =>
          logger.fatal("Empty ProxyLeaderInbound encountered.")
      }
    }
  }

  private def handlePhase2a(src: Transport#Address, phase2a: Phase2a): Unit = {
    val slotround = SlotRound(slot = phase2a.slot, round = phase2a.round)
    states.get(slotround) match {
      case Some(_) =>
        logger.debug(
          s"A ProxyLeader received a Phase2a in slot ${phase2a.slot} and " +
            s"round ${phase2a.round}, but it already received this Phase2a. " +
            s"The ProxyLeader is ignoring the message."
        )

      case None =>
        // Select the appropriate acceptor group, and then randomly select a
        // thrifty quorum of it. Relay the Phase2a message to the quorum
        val group = acceptors(phase2a.slot % config.numAcceptorGroups)
        val quorum = scala.util.Random.shuffle(group).take(config.quorumSize)
        if (options.flushPhase2asEveryN == 1) {
          quorum.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))
        } else {
          quorum.foreach(_.sendNoFlush(AcceptorInbound().withPhase2A(phase2a)))
          numPhase2asSentSinceLastFlush += 1
          if (numPhase2asSentSinceLastFlush >= options.flushPhase2asEveryN) {
            for (group <- acceptors; acceptor <- group) {
              acceptor.flush()
            }
            numPhase2asSentSinceLastFlush = 0
          }
        }

        // Update our state.
        states(slotround) = Pending(phase2a = phase2a, phase2bs = mutable.Map())
    }
  }

  private def handlePhase2b(src: Transport#Address, phase2b: Phase2b): Unit = {
    val slotround = SlotRound(slot = phase2b.slot, round = phase2b.round)
    states.get(slotround) match {
      case None =>
        logger.fatal(
          s"A ProxyLeader received a Phase2b in slot ${phase2b.slot} and " +
            s"round ${phase2b.round}, but it never sent a Phase2a in this " +
            s"slot and round."
        )

      case Some(Done) =>
        logger.debug(
          s"A ProxyLeader received a Phase2b in slot ${phase2b.slot} and " +
            s"round ${phase2b.round}, but it has already chosen a value in " +
            s"this slot and round. The Phase2b message is ignored."
        )

      case Some(pending: Pending) =>
        // Wait until we receive a quorum of Phase2bs.
        val phase2bs = pending.phase2bs
        phase2bs(phase2b.acceptorIndex) = phase2b
        if (phase2bs.size < config.quorumSize) {
          return
        }

        // Let the replicas know that the value has been chosen.
        /*replicas.foreach(
          _.send(
            ReplicaInbound().withChosen(
              Chosen(slot = phase2b.slot,
                     commandBatchOrNoop = pending.phase2a.commandBatchOrNoop)
            )
          )
        )*/

        // Update our state.
        states(slotround) = Done
    }
  }
}
