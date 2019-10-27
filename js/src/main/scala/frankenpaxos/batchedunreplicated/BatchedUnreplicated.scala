package frankenpaxos.batchedunreplicated

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class BatchedUnreplicated {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    batcherAddresses = Seq(
      JsTransportAddress("Batcher 1"),
      JsTransportAddress("Batcher 2")
    ),
    serverAddress = JsTransportAddress("Server"),
    proxyServerAddresses = Seq(
      JsTransportAddress("ProxyServer 1"),
      JsTransportAddress("ProxyServer 2")
    )
  )

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    new Client[JsTransport](
      address = JsTransportAddress(s"Client $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ClientOptions.default,
      metrics = new ClientMetrics(FakeCollectors)
    )
  }
  val client1 = clients(0)
  val client2 = clients(1)
  val client3 = clients(2)

  // Batchers.
  val batchers = for (address <- config.batcherAddresses) yield {
    new Batcher[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = BatcherOptions.default.copy(batchSize = 2),
      metrics = new BatcherMetrics(FakeCollectors)
    )
  }
  val batcher1 = batchers(0)
  val batcher2 = batchers(1)

  // Server.
  val server =
    new Server[JsTransport](
      address = config.serverAddress,
      transport = transport,
      logger = new JsLogger(),
      stateMachine = new AppendLog(),
      config = config,
      options = ServerOptions.default.copy(flushEveryN = 1),
      metrics = new ServerMetrics(FakeCollectors)
    )

  // ProxyServers.
  val proxyServers = for (address <- config.proxyServerAddresses) yield {
    new ProxyServer[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ProxyServerOptions.default.copy(flushEveryN = 2),
      metrics = new ProxyServerMetrics(FakeCollectors)
    )
  }
  val proxyServer1 = proxyServers(0)
  val proxyServer2 = proxyServers(1)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.batchedunreplicated.BatchedUnreplicated")
object BatchedUnreplicated {
  val BatchedUnreplicated = new BatchedUnreplicated();
}
