package frankenpaxos.heartbeat

import scala.scalajs.js.annotation._
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress

@JSExportAll
class Heartbeat {
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
  val options = HeartbeatOptions(
    failPeriod = java.time.Duration.ofMillis(1000),
    successPeriod = java.time.Duration.ofMillis(10000),
    numRetries = 1,
    networkDelayAlpha = 0.9
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
@JSExportTopLevel("frankenpaxos.heartbeat.TweenedHeartbeat")
object TweenedHeartbeat {
  val Heartbeat = new Heartbeat();
}
