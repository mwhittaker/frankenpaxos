package frankenpaxos.heartbeat

import scala.scalajs.js.annotation._
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress

@JSExportAll
class Heartbeat {
  val logger = new JsLogger()
  val transport = new JsTransport(logger);
  val options = HeartbeatOptions(
    failPeriod = java.time.Duration.ofMillis(1000),
    successPeriod = java.time.Duration.ofMillis(10000),
    numRetries = 1,
    networkDelayAlpha = 0.9
  )

  val addresses = for (name <- Seq("a", "b", "c", "d", "e"))
    yield JsTransportAddress(name)
  val participants = for (address <- addresses)
    yield
      new Participant[JsTransport](
        address = address,
        transport = transport,
        logger = new JsLogger(),
        addresses = addresses.filter(_ != address)
      )

  val a = participants(0)
  val b = participants(1)
  val c = participants(2)
  val d = participants(3)
  val e = participants(4)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.heartbeat.TweenedHeartbeat")
object TweenedHeartbeat {
  val Heartbeat = new Heartbeat();
}
