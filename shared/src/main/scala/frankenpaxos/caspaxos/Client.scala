package frankenpaxos.caspaxos

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
case class ClientOptions(
    resendClientRequestTimerPeriod: java.time.Duration,
    measureLatencies: Boolean
)

@JSExportAll
object ClientOptions {
  val default = ClientOptions(
    resendClientRequestTimerPeriod = java.time.Duration.ofSeconds(5),
    measureLatencies = true
  )
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_client_requests_total")
    .help("Total number of client requests sent to a leader.")
    .register()

  val responsesTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_client_responses_total")
    .labelNames("type")
    .help("Total number of responses received from a leader.")
    .register()

  val responsesLatency: Summary = collectors.summary
    .build()
    .name("caspaxos_client_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a response from a leader.")
    .register()

  val idleResponsesTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_client_idle_responses_total")
    .help("Total number of responses received while idle.")
    .register()

  val wrongIdResponsesTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_client_wrong_id_responses_total")
    .help("Total number of responses received with the wrong id.")
    .register()

  val resendClientRequestTotal: Counter = collectors.counter
    .build()
    .name("caspaxos_client_resend_client_request_total")
    .help("Total number of times the client resent client requests.")
    .register()
}

@JSExportAll
object ClientInboundSerializer extends ProtoSerializer[ClientInbound] {
  type A = ClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Client {
  val serializer = ClientInboundSerializer

  sealed trait State[Transport <: frankenpaxos.Transport[Transport]]

  case class Idle[Transport <: frankenpaxos.Transport[Transport]](
      id: Int
  ) extends State[Transport]

  case class Pending[Transport <: frankenpaxos.Transport[Transport]](
      id: Int,
      promise: Promise[Set[Int]],
      resendClientRequest: Transport#Timer
  ) extends State[Transport]
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: ClientOptions = ClientOptions.default,
    metrics: ClientMetrics = new ClientMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  import Client._
  override type InboundMessage = ClientInbound
  override def serializer = Client.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  private val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  // Random number generator.
  private val rand = new Random(seed)

  // Channels to leaders.
  val leaders: Seq[Chan[Leader[Transport]]] =
    for (adress <- config.leaderAddresses)
      yield chan[Leader[Transport]](address, Leader.serializer)

  @JSExport
  protected var state: State[Transport] = Idle(id = 0)

  // Helpers ///////////////////////////////////////////////////////////////////
  private def toIntSetProto(xs: Set[Int]): IntSet = IntSet(value = xs.toSeq)

  private def fromIntSetProto(xs: IntSet): Set[Int] = xs.value.toSet

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
      values: Set[Int],
      promise: Promise[Set[Int]]
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        // Send the client request.
        val clientRequest = ClientRequest(
          clientAddress = addressAsBytes,
          clientId = idle.id,
          intSet = toIntSetProto(values)
        )
        val leader = leaders(rand.nextInt(leaders.size))
        leader.send(LeaderInbound().withClientRequest(clientRequest))
        metrics.requestsTotal.inc()

        // Update our state.
        state = Pending(
          id = idle.id,
          promise = promise,
          resendClientRequest = makeResendClientRequest(clientRequest)
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
  private def makeResendClientRequest(
      clientRequest: ClientRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendClientRequest",
      options.resendClientRequestTimerPeriod,
      () => {
        metrics.resendClientRequestTotal.inc()
        val leader = leaders(rand.nextInt(leaders.size))
        leader.send(LeaderInbound().withClientRequest(clientRequest))
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
      inbound: ClientInbound
  ): Unit = {
    import ClientInbound.Request

    val label =
      inbound.request match {
        case Request.ClientReply(r) => "ClientReply"
        case Request.Empty => {
          logger.fatal("Empty ClientInbound encountered.")
        }
      }
    metrics.responsesTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientReply(r) => handleClientReply(src, r)
        case Request.Empty => {
          logger.fatal("Empty ClientInbound encountered.")
        }
      }
    }
  }

  private def handleClientReply(
      src: Transport#Address,
      clientReply: ClientReply
  ): Unit = {
    state match {
      case idle: Idle[Transport] =>
        logger.debug("Client received a ClientReply but is idle.")
        metrics.idleResponsesTotal.inc()

      case pending: Pending[Transport] =>
        if (clientReply.clientId != pending.id) {
          logger.debug(
            s"Client received a ClientReply with id ${clientReply.clientId} " +
              s"but is pending with id ${pending.id}."
          )
          metrics.wrongIdResponsesTotal.inc()
          return
        }

        pending.promise.success(fromIntSetProto(clientReply.value))
        pending.resendClientRequest.stop()
        state = Idle(id = pending.id + 1)
        metrics.responsesTotal.inc()
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(values: Set[Int]): Future[Set[Int]] = {
    val promise = Promise[Set[Int]]()
    transport.executionContext.execute(
      () => proposeImpl(values, promise)
    )
    promise.future
  }
}
