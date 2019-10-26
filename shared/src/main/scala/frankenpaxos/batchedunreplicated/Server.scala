package frankenpaxos.batchedunreplicated

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
    // A server flushes all of its channels to the proxy servers after every
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
    .name("batchedunreplicated_server_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("batchedunreplicated_server_requests_latency")
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
    config: Config[Transport],
    options: ServerOptions = ServerOptions.default,
    metrics: ServerMetrics = new ServerMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ServerInbound
  override val serializer = ServerInboundSerializer

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new scala.util.Random(seed)

  @JSExport
  protected val proxyServers: Seq[Chan[ProxyServer[Transport]]] =
    for (address <- config.proxyServerAddresses)
      yield chan[ProxyServer[Transport]](address, ProxyServer.serializer)

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
        case Request.ClientRequestBatch(_) => "ClientRequestBatch"
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequestBatch(r) => handleClientRequestBatch(src, r)
        case Request.Empty =>
          logger.fatal("Empty ServerInbound encountered.")
      }
    }
  }

  private def handleClientRequestBatch(
      src: Transport#Address,
      clientRequestBatch: ClientRequestBatch
  ): Unit = {
    val results = mutable.Buffer[Result]()

    // Execute the commands.
    for (command <- clientRequestBatch.command) {
      val result =
        ByteString.copyFrom(stateMachine.run(command.command.toByteArray()))
      results += Result(
        clientAddress = command.clientAddress,
        commandId = command.commandId,
        result = result
      )
    }

    // Forward the results to a randomly selected proxy server.
    val proxyServer = proxyServers(rand.nextInt(proxyServers.size))
    val clientReplyBatch = ClientReplyBatch(result = results.toSeq)
    if (options.flushEveryN == 1) {
      timed("handleClientRequestBatch/send") {
        proxyServer.send(
          ProxyServerInbound().withClientReplyBatch(clientReplyBatch)
        )
      }
    } else {
      timed("handleClientRequestBatch/sendNoFlush") {
        proxyServer.sendNoFlush(
          ProxyServerInbound().withClientReplyBatch(clientReplyBatch)
        )
      }
      numMessagesSinceLastFlush += 1
      if (numMessagesSinceLastFlush >= options.flushEveryN) {
        timed("handleClientRequestBatch/flush") {
          proxyServers.foreach(_.flush())
        }
        numMessagesSinceLastFlush = 0
      }
    }
  }
}
