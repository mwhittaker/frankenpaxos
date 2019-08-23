package frankenpaxos.simplegcbpaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
case class CasClientOptions(
    resendCasClientRequestTimerPeriod: java.time.Duration,
    measureLatencies: Boolean
)

@JSExportAll
object CasClientOptions {
  val default = CasClientOptions(
    resendCasClientRequestTimerPeriod = java.time.Duration.ofSeconds(5),
    measureLatencies = true
  )
}

@JSExportAll
class CasClientMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_client_requests_total")
    .help("Total number of client requests sent to a leader.")
    .register()

  val responsesTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_client_responses_total")
    .labelNames("type")
    .help("Total number of responses received from a leader.")
    .register()

  val responsesLatency: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_cas_client_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a response from a leader.")
    .register()

  val idleResponsesTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_client_idle_responses_total")
    .help("Total number of responses received while idle.")
    .register()

  val wrongIdResponsesTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_client_wrong_id_responses_total")
    .help("Total number of responses received with the wrong id.")
    .register()

  val resendClientRequestTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_cas_client_resend_client_request_total")
    .help("Total number of times the client resent client requests.")
    .register()
}

@JSExportAll
object CasClientInboundSerializer extends ProtoSerializer[CasClientInbound] {
  type A = CasClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object CasClient {
  val serializer = CasClientInboundSerializer

  @JSExportAll
  sealed trait State[Transport <: frankenpaxos.Transport[Transport]]

  @JSExportAll
  case class Idle[Transport <: frankenpaxos.Transport[Transport]](
      id: Int
  ) extends State[Transport]

  @JSExportAll
  case class Pending[Transport <: frankenpaxos.Transport[Transport]](
      id: Int,
      promise: Promise[VertexIdPrefixSet],
      resendCasClientRequest: Transport#Timer
  ) extends State[Transport]
}

@JSExportAll
class CasClient[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: CasClientOptions = CasClientOptions.default,
    metrics: CasClientMetrics = new CasClientMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  import CasClient._
  override type InboundMessage = CasClientInbound
  override def serializer = CasClient.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  private val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // Random number generator.
  private val rand = new Random(seed)

  // Channels to leaders.
  private val leaders: Seq[Chan[CasLeader[Transport]]] =
    for (address <- config.casLeaderAddresses)
      yield chan[CasLeader[Transport]](address, CasLeader.serializer)

  @JSExport
  protected var state: State[Transport] = Idle(id = 0)

  // Helpers ///////////////////////////////////////////////////////////////////
  private def timed[T](label: String)(e: => T): T = {
    if (options.measureLatencies) {
      val startNanos = System.nanoTime
      val x = e
      val stopNanos = System.nanoTime
      metrics.responsesLatency
        .labels(label)
        .observe((stopNanos - startNanos).toDouble / 1000000)
      x
    } else {
      e
    }
  }

  private def proposeImpl(
      watermark: VertexIdPrefixSet,
      promise: Promise[VertexIdPrefixSet]
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        // Send the client request.
        val clientRequest = CasClientRequest(
          clientAddress = addressAsBytes,
          clientId = idle.id,
          watermark = watermark.toProto()
        )
        val leader = leaders(rand.nextInt(leaders.size))
        leader.send(CasLeaderInbound().withCasClientRequest(clientRequest))
        metrics.requestsTotal.inc()

        // Update our state.
        state = Pending(
          id = idle.id,
          promise = promise,
          resendCasClientRequest = makeResendCasClientRequest(clientRequest)
        )

      case pending: Pending[Transport] =>
        promise.failure(
          new IllegalStateException(
            s"You attempted to propose a value, but a value is already " +
              s"pending. A client can only have one pending request at a time."
          )
        )
    }
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendCasClientRequest(
      clientRequest: CasClientRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendCasClientRequest",
      options.resendCasClientRequestTimerPeriod,
      () => {
        metrics.resendClientRequestTotal.inc()
        val leader = leaders(rand.nextInt(leaders.size))
        leader.send(CasLeaderInbound().withCasClientRequest(clientRequest))
        metrics.requestsTotal.inc()
        t.start()
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: CasClientInbound
  ): Unit = {
    import CasClientInbound.Request

    val label =
      inbound.request match {
        case Request.CasClientReply(r) => "CasClientReply"
        case Request.Empty => {
          logger.fatal("Empty CasClientInbound encountered.")
        }
      }
    metrics.responsesTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.CasClientReply(r) => handleCasClientReply(src, r)
        case Request.Empty => {
          logger.fatal("Empty CasClientInbound encountered.")
        }
      }
    }
  }

  private def handleCasClientReply(
      src: Transport#Address,
      clientReply: CasClientReply
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        logger.debug("CasClient received a CasClientReply but is idle.")
        metrics.idleResponsesTotal.inc()

      case pending: Pending[Transport] =>
        if (clientReply.clientId != pending.id) {
          logger.debug(
            s"CasClient received a CasClientReply with id " +
              s"${clientReply.clientId} but is pending with id ${pending.id}."
          )
          metrics.wrongIdResponsesTotal.inc()
          return
        }

        pending.promise.success(
          VertexIdPrefixSet.fromProto(clientReply.watermark)
        )
        pending.resendCasClientRequest.stop()
        state = Idle(id = pending.id + 1)
        metrics.responsesTotal.inc()
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(watermark: VertexIdPrefixSet): Future[VertexIdPrefixSet] = {
    val promise = Promise[VertexIdPrefixSet]()
    transport.executionContext.execute(
      () => proposeImpl(watermark, promise)
    )
    promise.future
  }
}
