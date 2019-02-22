package frankenpaxos.election

import scala.scalajs.js.annotation._
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress

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
    pingPeriod = java.time.Duration.ofMillis(1500),
    noPingTimeoutMin = java.time.Duration.ofMillis(2500),
    noPingTimeoutMax = java.time.Duration.ofMillis(3500),
    notEnoughVotesTimeoutMin = java.time.Duration.ofMillis(2500),
    notEnoughVotesTimeoutMax = java.time.Duration.ofMillis(3500)
  )

  val a = new Participant[JsTransport](aAddress,
                                       transport,
                                       aLogger,
                                       addresses,
                                       options = options)
  val b = new Participant[JsTransport](bAddress,
                                       transport,
                                       bLogger,
                                       addresses,
                                       options = options)
  val c = new Participant[JsTransport](cAddress,
                                       transport,
                                       cLogger,
                                       addresses,
                                       options = options)
  val d = new Participant[JsTransport](dAddress,
                                       transport,
                                       dLogger,
                                       addresses,
                                       options = options)
  val e = new Participant[JsTransport](eAddress,
                                       transport,
                                       eLogger,
                                       addresses,
                                       options = options)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.election.SimulatedLeaderElection")
object SimulatedLeaderElection {
  val LeaderElection = new LeaderElection();
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.election.ClickthroughLeaderElection")
object ClickthroughLeaderElection {
  val LeaderElection = new LeaderElection();
}
