package frankenpaxos.unanimousbpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.depgraph.ScalaGraphDependencyGraph
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.statemachine.AppendLog
import scala.scalajs.js.annotation._

@JSExportAll
class UnanimousBPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2")
    ),
    depServiceNodeAddresses = Seq(
      JsTransportAddress("Dep Service Node 1"),
      JsTransportAddress("Dep Service Node 2"),
      JsTransportAddress("Dep Service Node 3")
    ),
    acceptorAddresses = Seq(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
    )
  )

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    new Client[JsTransport](
      address = JsTransportAddress(s"Client $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = ClientOptions.default.copy(
        reproposePeriod = java.time.Duration.ofSeconds(10)
      ),
      metrics = new ClientMetrics(FakeCollectors)
    )
  }
  val client1 = clients(0)
  val client2 = clients(1)
  val client3 = clients(2)

  // Leaders.
  val leaders = for (i <- 1 to 2) yield {
    new Leader[JsTransport](
      address = JsTransportAddress(s"Leader $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      stateMachine = new AppendLog(),
      dependencyGraph = new ScalaGraphDependencyGraph(),
      options = LeaderOptions.default.copy(
        resendDependencyRequestsTimerPeriod = java.time.Duration.ofSeconds(5),
        resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(5),
        resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(5),
        recoverVertexTimerMinPeriod = java.time.Duration.ofSeconds(10),
        recoverVertexTimerMaxPeriod = java.time.Duration.ofSeconds(20)
      ),
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }
  val leader1 = leaders(0)
  val leader2 = leaders(1)

  // DepServiceNodes.
  val depServiceNodes = for (i <- 1 to 3) yield {
    new DepServiceNode[JsTransport](
      address = JsTransportAddress(s"Dep Service Node $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      stateMachine = new AppendLog(),
      options = DepServiceNodeOptions.default.copy(),
      metrics = new DepServiceNodeMetrics(FakeCollectors)
    )
  }
  val depServiceNode1 = depServiceNodes(0)
  val depServiceNode2 = depServiceNodes(1)
  val depServiceNode3 = depServiceNodes(2)

  // Acceptors.
  val acceptors = for (i <- 1 to 3) yield {
    new Acceptor[JsTransport](
      address = JsTransportAddress(s"Acceptor $i"),
      transport = transport,
      logger = new JsLogger(),
      config = config,
      options = AcceptorOptions.default.copy(),
      metrics = new AcceptorMetrics(FakeCollectors)
    )
  }
  val acceptor1 = acceptors(0)
  val acceptor2 = acceptors(1)
  val acceptor3 = acceptors(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.unanimousbpaxos.UnanimousBPaxos")
object UnanimousBPaxos {
  val UnanimousBPaxos = new UnanimousBPaxos();
}
