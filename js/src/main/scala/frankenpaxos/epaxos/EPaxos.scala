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
    replicaAddresses = Seq(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2"),
      JsTransportAddress("Replica 3"),
      JsTransportAddress("Replica 4"),
      JsTransportAddress("Replica 5")
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

  // Replicas.
  val replicas = for (i <- 1 to 5) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Replica $i")
    val options = ReplicaOptions.default.copy(
      resendPreAcceptsTimerPeriod = java.time.Duration.ofSeconds(3),
      defaultToSlowPathTimerPeriod = java.time.Duration.ofSeconds(5),
      resendAcceptsTimerPeriod = java.time.Duration.ofSeconds(5),
      resendPreparesTimerPeriod = java.time.Duration.ofSeconds(3),
      recoverInstanceTimerMinPeriod = java.time.Duration.ofSeconds(10),
      recoverInstanceTimerMaxPeriod = java.time.Duration.ofSeconds(20),
      executeGraphBatchSize = 1,
      executeGraphTimerPeriod = java.time.Duration.ofSeconds(10)
    )
    val replica = new Replica[JsTransport](address,
                                           transport,
                                           logger,
                                           config,
                                           new Register(),
                                           new ScalaGraphDependencyGraph(),
                                           options,
                                           new ReplicaMetrics(FakeCollectors))
    (logger, replica)
  }
  val (replica1logger, replica1) = replicas(0)
  val (replica2logger, replica2) = replicas(1)
  val (replica3logger, replica3) = replicas(2)
  val (replica4logger, replica4) = replicas(3)
  val (replica5logger, replica5) = replicas(4)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.epaxos.TweenedEPaxos")
object TweenedEPaxos {
  val EPaxos = new EPaxos();
}
