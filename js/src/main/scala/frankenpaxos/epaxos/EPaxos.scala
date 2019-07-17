package frankenpaxos.epaxos

import InstanceHelpers.instanceOrdering
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.depgraph.ScalaGraphDependencyGraph
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.statemachine.Register
import scala.scalajs.js.annotation._

@JSExportAll
class EPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 2,
    leaderAddresses = Seq(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2"),
      JsTransportAddress("Leader 3"),
      JsTransportAddress("Leader 4"),
      JsTransportAddress("Leader 5")
    )
  )

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Client $i")
    val options = ClientOptions.default.copy(
      reproposePeriod = java.time.Duration.ofSeconds(10)
    )
    val client =
      new Client[JsTransport](address,
                              transport,
                              logger,
                              config,
                              options,
                              new ClientMetrics(FakeCollectors))
    (logger, client)
  }
  val (client1logger, client1) = clients(0)
  val (client2logger, client2) = clients(1)
  val (client3logger, client3) = clients(2)

  // Leaders.
  val leaders = for (i <- 1 to 5) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Leader $i")
    val options = LeaderOptions.default.copy(
      resendPreAcceptsTimerPeriod = java.time.Duration.ofSeconds(3),
      defaultToSlowPathTimerPeriod = java.time.Duration.ofSeconds(5),
      resendAcceptsTimerPeriod = java.time.Duration.ofSeconds(5),
      resendPreparesTimerPeriod = java.time.Duration.ofSeconds(3),
      recoverInstanceTimerMinPeriod = java.time.Duration.ofSeconds(10),
      recoverInstanceTimerMaxPeriod = java.time.Duration.ofSeconds(20),
      executeGraphBatchSize = 1,
      executeGraphTimerPeriod = java.time.Duration.ofSeconds(10)
    )
    val leader = new Leader[JsTransport](address,
                                         transport,
                                         logger,
                                         config,
                                         new Register(),
                                         new ScalaGraphDependencyGraph(),
                                         options,
                                         new LeaderMetrics(FakeCollectors))
    (logger, leader)
  }
  val (leader1logger, leader1) = leaders(0)
  val (leader2logger, leader2) = leaders(1)
  val (leader3logger, leader3) = leaders(2)
  val (leader4logger, leader4) = leaders(3)
  val (leader5logger, leader5) = leaders(4)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.epaxos.TweenedEPaxos")
object TweenedEPaxos {
  val EPaxos = new EPaxos();
}
