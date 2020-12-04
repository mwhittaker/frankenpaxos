package frankenpaxos.craq

import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.ReadableAppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class Craq(batch: Boolean) {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    chainNodeAddresses = Seq(
      JsTransportAddress("ChainNode 1"),
      JsTransportAddress("ChainNode 2"),
      JsTransportAddress("ChainNode 3")
    ),
    // numBatchers acts as a boolean of whether to batch or not.
    numBatchers = if (batch) 1 else 0
  )

  // Clients.
  val clients = for (i <- 1 to 4) yield {
    new Client[JsTransport](
      address = JsTransportAddress(s"Client $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ClientOptions.default.copy(
        resendClientRequestPeriod = java.time.Duration.ofSeconds(30)
      ),
      metrics = new ClientMetrics(FakeCollectors)
    )
  }
  val client1 = clients(0)
  val client2 = clients(1)
  val client3 = clients(2)
  val client4 = clients(3)

  // ChainNodes.
  val chainNodes = for (address <- config.chainNodeAddresses) yield {
    new ChainNode[JsTransport](
      address = address,
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ChainNodeOptions.default,
      metrics = new ChainNodeMetrics(FakeCollectors)
    )
  }
  val chainNode1 = chainNodes(0)
  val chainNode2 = chainNodes(1)
  val chainNode3 = chainNodes(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.craq.Craq")
object Craq {
  val Craq = new Craq(batch = false);
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.craq.BatchedCraq")
object BatchedCraq {
  val Craq = new Craq(batch = true);
}
