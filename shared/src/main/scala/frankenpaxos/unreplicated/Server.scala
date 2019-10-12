package frankenpaxos.unreplicated

import collection.mutable
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.statemachine.StateMachine
import scala.scalajs.js.annotation._

@JSExportAll
object ServerInboundSerializer extends ProtoSerializer[ServerInbound] {
  type A = ServerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Server {
  val serializer = ServerInboundSerializer
}

@JSExportAll
case class ServerOptions(
    // A server flushes all of its channels to the clients after every
    // `flushEveryN` commands.
    flushEveryN: Int,
    measureLatencies: Boolean
)

@JSExportAll
object ServerOptions {
  val default = ServerOptions(
    flushEveryN = 1,
    measureLatencies = true
  )
}

@JSExportAll
class ServerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("unreplicated_server_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("unreplicated_server_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class Server[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    // Public for Javascript visualizations.
    val stateMachine: StateMachine,
    options: ServerOptions = ServerOptions.default,
    metrics: ServerMetrics = new ServerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ServerInbound
  override val serializer = ServerInboundSerializer

  // Fields ////////////////////////////////////////////////////////////////////
  private val clients =
    mutable.Map[Transport#Address, Chan[Client[Transport]]]()

  // The number of messages since the last flush.
  private var numMessagesSinceLastFlush: Int = 0

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
    import ServerInbound.Request

    val label =
      inbound.request match {
        case Request.ClientRequest(_) => "ClientRequest"
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r) => handleClientRequest(src, r)
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    // Execute the command.
    val result =
      ByteString.copyFrom(stateMachine.run(clientRequest.command.toByteArray()))
    val clientReply =
      ClientReply(commandId = clientRequest.commandId, result = result)

    // Return the result.
    val client = clients.getOrElseUpdate(
      src,
      chan[Client[Transport]](src, Client.serializer)
    )
    if (options.flushEveryN == 1) {
      client.send(ClientInbound().withClientReply(clientReply))
    } else {
      client.sendNoFlush(ClientInbound().withClientReply(clientReply))
      numMessagesSinceLastFlush += 1
      if (numMessagesSinceLastFlush >= options.flushEveryN) {
        clients.values.foreach(_.flush())
        numMessagesSinceLastFlush = 0
      }
    }
  }
}
