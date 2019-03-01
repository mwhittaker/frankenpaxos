package frankenpaxos.epaxos

import scala.scalajs.js.annotation._
import frankenpaxos._

@JSExportAll
class EPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger)

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    replicaAddresses = Seq(
      JsTransportAddress("Replica 1"),
      JsTransportAddress("Replica 2"),
      JsTransportAddress("Replica 3")
    ),
  )

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Client $i")
    val client = new Client[JsTransport](address, transport, logger, config)
    (logger, client)
  }
  val (client1logger, client1) = clients(0)
  val (client2logger, client2) = clients(1)
  val (client3logger, client3) = clients(2)

  // Replicas.
  val replicas = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Replica $i")
    val replica = new Replica[JsTransport](address, transport, logger, config)
    (logger, replica)
  }
  val (replica1logger, replica1) = replicas(0)
  val (replica2logger, replica2) = replicas(1)
  val (replica3logger, replica3) = replicas(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.epaxos.SimulatedEPaxos")
object SimulatedEPaxos {
  val EPaxos = new EPaxos();
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.epaxos.ClickthroughEPaxos")
object ClickthroughEPaxos {
  val EPaxos = new EPaxos();
}
