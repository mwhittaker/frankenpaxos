package frankenpaxos.election.basic

import scala.scalajs.js.annotation._
import frankenpaxos.JsLogger
import frankenpaxos.JsTransport
import frankenpaxos.JsTransportAddress

@JSExportAll
class Election {
  val logger = new JsLogger()
  val transport = new JsTransport(logger)
  val addresses = Seq("a", "b", "c", "d", "e").map(JsTransportAddress)
  val options = ElectionOptions(
    pingPeriod = java.time.Duration.ofMillis(1000),
    noPingTimeoutMin = java.time.Duration.ofMillis(2000),
    noPingTimeoutMax = java.time.Duration.ofMillis(5000)
  )

  val partipants = for (address <- addresses) yield {
    val logger = new JsLogger()
    val partipant =
      new Participant[JsTransport](address,
                                   transport,
                                   logger,
                                   addresses,
                                   options = options)
    partipant.register((index) => {
      logger.debug(s"${addresses(index)} is the new leader!")
    })
    partipant
  }

  val a = partipants(0)
  val b = partipants(1)
  val c = partipants(2)
  val d = partipants(3)
  val e = partipants(4)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.election.basic.TweenedElection")
object TweenedElection {
  val Election = new Election();
}
