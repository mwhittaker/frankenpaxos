package frankenpaxos.fastmultipaxos

import frankenpaxos.Actor
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
class FastMultiPaxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger);

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    leaderAddresses = List(
      JsTransportAddress("Leader 1"),
      JsTransportAddress("Leader 2")
    ),
    leaderElectionAddresses = List(
      JsTransportAddress("Leader Election 1"),
      JsTransportAddress("Leader Election 2")
    ),
    leaderHeartbeatAddresses = List(
      JsTransportAddress("Leader Heartbeat 1"),
      JsTransportAddress("Leader Heartbeat 2")
    ),
    acceptorAddresses = List(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
    ),
    acceptorHeartbeatAddresses = List(
      JsTransportAddress("Acceptor Heartbeat 1"),
      JsTransportAddress("Acceptor Heartbeat 2"),
      JsTransportAddress("Acceptor Heartbeat 3")
    ),
    roundSystem = new RoundSystem.MixedRoundRobin(2)
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

  // Leaders.
  val leaders = for (i <- 1 to 2) yield {
    val leader = new Leader[JsTransport](JsTransportAddress(s"Leader $i"),
                                         transport,
                                         new JsLogger(),
                                         config)
    (logger, leader)
  }
  val (leader1logger, leader1) = leaders(0)
  val (leader2logger, leader2) = leaders(1)

  // Acceptors.
  val acceptors = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Acceptor $i")
    val acceptor = new Acceptor[JsTransport](address, transport, logger, config)
    (logger, acceptor)
  }
  val (acceptor1logger, acceptor1) = acceptors(0)
  val (acceptor2logger, acceptor2) = acceptors(1)
  val (acceptor3logger, acceptor3) = acceptors(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.fastpaxos.SimulatedFastMultiPaxos")
object SimulatedFastMultiPaxos {
  val FastMultiPaxos = new FastMultiPaxos();
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.fastpaxos.ClickthroughFastMultiPaxos")
object ClickthroughFastMultiPaxos {
  val FastMultiPaxos = new FastMultiPaxos();
}
