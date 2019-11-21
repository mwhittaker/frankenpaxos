package frankenpaxos.matchmakerpaxos

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.roundsystem.RoundSystem
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import scala.util.Random

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
}

@JSExportAll
case class ClientOptions(
    resendClientRequestPeriod: java.time.Duration
)

@JSExportAll
object ClientOptions {
  val default = ClientOptions(
    resendClientRequestPeriod = java.time.Duration.ofSeconds(10)
  )
}

@JSExportAll
class ClientMetrics(collectors: Collectors) {}

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
  config.checkValid()

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ClientInbound
  override val serializer = ClientInboundSerializer

  @JSExportAll
  sealed trait State

  @JSExportAll
  case object Inactive extends State

  @JSExportAll
  case class Pending(
      promises: mutable.Buffer[Promise[String]],
      resendClientRequest: Transport#Timer
  ) extends State

  @JSExportAll
  case class Chosen(v: String) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  // Leader channels.
  private val leaders: Seq[Chan[Leader[Transport]]] =
    for (address <- config.leaderAddresses)
      yield chan[Leader[Transport]](address, Leader.serializer)

  // Public for testing.
  var state: State = Inactive

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendClientRequestTimer(
      clientRequest: ClientRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendClientRequest ",
      options.resendClientRequestPeriod,
      () => {
        val leader = leaders(rand.nextInt(leaders.size))
        leader.send(LeaderInbound().withClientRequest(clientRequest))
        t.start()
      }
    )
    t.start()
    t
  }

  private def proposeImpl(v: String, promise: Promise[String]): Unit = {
    state match {
      case Inactive =>
        // Send request.
        val clientRequest = ClientRequest(v = v)
        val leader = leaders(rand.nextInt(leaders.size))
        leader.send(LeaderInbound().withClientRequest(clientRequest))

        // Update state.
        state = Pending(
          promises = mutable.Buffer(promise),
          resendClientRequest = makeResendClientRequestTimer(clientRequest)
        )

      case pending: Pending =>
        pending.promises += promise

      case chosen: Chosen =>
        promise.success(chosen.v)
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import ClientInbound.Request
    inbound.request match {
      case Request.ClientReply(r) => handleClientReply(src, r)
      case Request.Empty =>
        logger.fatal("Empty ClientInbound encountered.")
    }
  }

  private def handleClientReply(
      src: Transport#Address,
      clientReply: ClientReply
  ): Unit = {
    state match {
      case Inactive =>
        state = Chosen(clientReply.chosen)

      case pending: Pending =>
        pending.promises.foreach(_.success(clientReply.chosen))
        pending.resendClientRequest.stop()
        state = Chosen(clientReply.chosen)

      case chosen: Chosen =>
        logger.checkEq(clientReply.chosen, chosen.v)
    }
  }

  // Interface /////////////////////////////////////////////////////////////////
  def propose(value: String): Future[String] = {
    val promise = Promise[String]()
    transport.executionContext.execute(() => proposeImpl(value, promise))
    promise.future
  }
}
