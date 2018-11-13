package zeno.examples.js

import scala.collection.mutable
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.JsLogger
import zeno.JsTransport
import zeno.JsTransportAddress
import zeno.examples.PaxosAcceptorActor
import zeno.examples.PaxosClientActor
import zeno.examples.PaxosProposerActor
import zeno.examples.PaxosConfig

@JSExportAll
class Paxos {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger);

  // Configuration.
  val config = PaxosConfig[JsTransport](
    f = 1,
    proposerAddresses = List(
      JsTransportAddress("Proposer 1"),
      JsTransportAddress("Proposer 2")
    ),
    acceptorAddresses = List(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
    )
  )

  // Clients.
  val client1Logger = new JsLogger()
  val client1 = new PaxosClientActor[JsTransport](
    JsTransportAddress("Client 1"),
    transport,
    client1Logger,
    config
  )

  val client2Logger = new JsLogger()
  val client2 = new PaxosClientActor[JsTransport](
    JsTransportAddress("Client 2"),
    transport,
    client2Logger,
    config
  )

  val client3Logger = new JsLogger()
  val client3 = new PaxosClientActor[JsTransport](
    JsTransportAddress("Client 3"),
    transport,
    client3Logger,
    config
  )

  // Proposers.
  val proposer1Logger = new JsLogger()
  val proposer1 = new PaxosProposerActor[JsTransport](
    JsTransportAddress("Proposer 1"),
    transport,
    proposer1Logger,
    config
  )

  val proposer2Logger = new JsLogger()
  val proposer2 = new PaxosProposerActor[JsTransport](
    JsTransportAddress("Proposer 2"),
    transport,
    proposer2Logger,
    config
  )

  // Acceptors.
  val acceptor1Logger = new JsLogger()
  val acceptor1 = new PaxosAcceptorActor[JsTransport](
    JsTransportAddress("Acceptor 1"),
    transport,
    acceptor1Logger,
    config
  )

  val acceptor2Logger = new JsLogger()
  val acceptor2 = new PaxosAcceptorActor[JsTransport](
    JsTransportAddress("Acceptor 2"),
    transport,
    acceptor2Logger,
    config
  )

  val acceptor3Logger = new JsLogger()
  val acceptor3 = new PaxosAcceptorActor[JsTransport](
    JsTransportAddress("Acceptor 3"),
    transport,
    acceptor3Logger,
    config
  )
}

@JSExportAll
@JSExportTopLevel("zeno.examples.js.SimulatedPaxos")
object SimulatedPaxos {
  val Paxos = new Paxos();
}

@JSExportAll
@JSExportTopLevel("zeno.examples.js.ClickthroughPaxos")
object ClickthroughPaxos {
  val Paxos = new Paxos();
}
