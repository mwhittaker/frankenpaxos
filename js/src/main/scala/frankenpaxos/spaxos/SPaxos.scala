package frankenpaxos.spaxos
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.statemachine.{AppendLog, StateMachine}

import scala.scalajs.js.annotation._

@JSExportAll
class SPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger);

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    replicaAddresses = List(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2"),
      JsTransportAddress("Replica 3"),
      JsTransportAddress("Replica 4"),
      JsTransportAddress("Replica 5")
    ),
  )

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Client $i")
    val client = new Client[JsTransport](address,
                                         transport,
                                         logger,
                                         config,
                                         ClientOptions.default,
                                         new ClientMetrics(FakeCollectors))
    (logger, client)
  }
  val (client1logger, client1) = clients(0)
  val (client2logger, client2) = clients(1)
  val (client3logger, client3) = clients(2)

  // Leaders.
  val replicaOptions = ReplicaOptions.default.copy(
    phase2aMaxBufferSize = 3,
    phase2aBufferFlushPeriod = java.time.Duration.ofSeconds(5),
  )
  val replicas = for (i <- 1 to 5) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Replica $i")
    val replica = new Replica[JsTransport](address,
                                         transport,
                                         logger,
                                         config,
      new AppendLog(),
                                        ReplicaOptions.default, new ReplicaMetrics(FakeCollectors))
    (logger, replica)
  }
  val (replica1logger, replica1) = replicas(0)
  val (replica2logger, replica2) = replicas(1)
  val (replica3logger, replica3) = replicas(2)
  val (replica4logger, replica4) = replicas(3)
  val (replica5logger, replica5) = replicas(4)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.spaxos.TweenedSPaxos")
object TweenedSPaxos {
  val SPaxos = new SPaxos()
}
