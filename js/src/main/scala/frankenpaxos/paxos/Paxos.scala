package frankenpaxos.paxos

import scala.collection.mutable
import scala.scalajs.js.annotation._
import frankenpaxos.Actor
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress

@JSExportAll
class Paxos {
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
    acceptorAddresses = List(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
    )
  )

  // Clients.
  val client1Logger = new JsLogger()
  val client1 = new Client[JsTransport](
    JsTransportAddress("Client 1"),
    transport,
    client1Logger,
    config
  )

  val client2Logger = new JsLogger()
  val client2 = new Client[JsTransport](
    JsTransportAddress("Client 2"),
    transport,
    client2Logger,
    config
  )

  val client3Logger = new JsLogger()
  val client3 = new Client[JsTransport](
    JsTransportAddress("Client 3"),
    transport,
    client3Logger,
    config
  )

  // Leaders.
  val leader1Logger = new JsLogger()
  val leader1 = new Leader[JsTransport](
    JsTransportAddress("Leader 1"),
    transport,
    leader1Logger,
    config
  )

  val leader2Logger = new JsLogger()
  val leader2 = new Leader[JsTransport](
    JsTransportAddress("Leader 2"),
    transport,
    leader2Logger,
    config
  )

  // Acceptors.
  val acceptor1Logger = new JsLogger()
  val acceptor1 = new Acceptor[JsTransport](
    JsTransportAddress("Acceptor 1"),
    transport,
    acceptor1Logger,
    config
  )

  val acceptor2Logger = new JsLogger()
  val acceptor2 = new Acceptor[JsTransport](
    JsTransportAddress("Acceptor 2"),
    transport,
    acceptor2Logger,
    config
  )

  val acceptor3Logger = new JsLogger()
  val acceptor3 = new Acceptor[JsTransport](
    JsTransportAddress("Acceptor 3"),
    transport,
    acceptor3Logger,
    config
  )
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.paxos.TweenedPaxos")
object TweenedPaxos {
  val Paxos = new Paxos();
}
