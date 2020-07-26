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
    batcherAddresses = if (batch) {
      Seq(
        JsTransportAddress("Batcher 1"),
        JsTransportAddress("Batcher 2")
      )
    } else {
      Seq()
    },
    readBatcherAddresses = if (batch) {
      Seq(
        JsTransportAddress("ReadBatcher 1"),
        JsTransportAddress("ReadBatcher 2")
      )
    } else {
      Seq()
    },
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2")
    ),
    leaderElectionAddresses = Seq(
      JsTransportAddress("LeaderElection 1"),
      JsTransportAddress("LeaderElection 2")
    ),
    proxyLeaderAddresses = Seq(
      JsTransportAddress("ProxyLeader 1"),
      JsTransportAddress("ProxyLeader 2")
    ),
    acceptorAddresses = Seq(
      Seq(
        JsTransportAddress("Acceptor A1"),
        JsTransportAddress("Acceptor A2"),
        JsTransportAddress("Acceptor A3")
      ),
      Seq(
        JsTransportAddress("Acceptor B1"),
        JsTransportAddress("Acceptor B2"),
        JsTransportAddress("Acceptor B3")
      )
    ),
    replicaAddresses = Seq(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2")
    ),
    proxyReplicaAddresses = Seq(
      JsTransportAddress("ProxyReplica 1"),
      JsTransportAddress("ProxyReplica 2")
    ),
    chainNodeAddresses = Seq(
      JsTransportAddress("ChainNode 1"),
      JsTransportAddress("ChainNode 2"),
      JsTransportAddress("ChainNode 3")
    ),
    distributionScheme = Hash
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

/*@JSExportAll
@JSExportTopLevel("foobar")
object Foo {
  val bar = 1
}*/

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
