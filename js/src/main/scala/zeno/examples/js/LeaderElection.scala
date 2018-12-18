package zeno.examples.js

// import scala.collection.mutable
import scala.scalajs.js.annotation._
// import zeno.Actor
import zeno.JsLogger
import zeno.JsTransport
import zeno.JsTransportAddress
// import zeno.examples.LeaderElectionClientActor
import zeno.examples.LeaderElectionActor
import zeno.examples.LeaderElectionOptions

@JSExportAll
class LeaderElection {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger);

  // Addresses.
  val aAddress = JsTransportAddress("a")
  val bAddress = JsTransportAddress("b")
  val cAddress = JsTransportAddress("c")
  val dAddress = JsTransportAddress("d")
  val eAddress = JsTransportAddress("e")
  val addresses = Set(aAddress, bAddress, cAddress, dAddress, eAddress)

  // Loggers.
  val aLogger = new JsLogger()
  val bLogger = new JsLogger()
  val cLogger = new JsLogger()
  val dLogger = new JsLogger()
  val eLogger = new JsLogger()

  // Nodes.
  val options = LeaderElectionOptions(
    pingPeriod = java.time.Duration.ofSeconds(1),
    noPingTimeoutMin = java.time.Duration.ofSeconds(2),
    noPingTimeoutMax = java.time.Duration.ofSeconds(3),
    notEnoughVotesTimeoutMin = java.time.Duration.ofSeconds(2),
    notEnoughVotesTimeoutMax = java.time.Duration.ofSeconds(3)
  )

  val a = new LeaderElectionActor[JsTransport](
    aAddress,
    transport,
    aLogger,
    addresses,
    options
  )
  val b = new LeaderElectionActor[JsTransport](
    bAddress,
    transport,
    bLogger,
    addresses,
    options
  )
  val c = new LeaderElectionActor[JsTransport](
    cAddress,
    transport,
    cLogger,
    addresses,
    options
  )
  val d = new LeaderElectionActor[JsTransport](
    dAddress,
    transport,
    dLogger,
    addresses,
    options
  )
  val e = new LeaderElectionActor[JsTransport](
    eAddress,
    transport,
    eLogger,
    addresses,
    options
  )
}

@JSExportAll
@JSExportTopLevel("zeno.examples.js.SimulatedLeaderElection")
object SimulatedLeaderElection {
  val LeaderElection = new LeaderElection();
}

@JSExportAll
@JSExportTopLevel("zeno.examples.js.ClickthroughLeaderElection")
object ClickthroughLeaderElection {
  val LeaderElection = new LeaderElection();
}
