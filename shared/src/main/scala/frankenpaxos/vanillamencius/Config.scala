package frankenpaxos.vanillamencius

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    serverAddresses: Seq[Transport#Address],
    heartbeatAddresses: Seq[Transport#Address]
) {
  def checkValid(): Unit = {
    require(f >= 1, s"f must be >= 1. It's $f.")
    require(
      serverAddresses.size == 2 * f + 1,
      s"There must be 2f+1 (${2 * f + 1}) servers. " +
        s"There's only ${serverAddresses.size}."
    )
    require(
      heartbeatAddresses.size == serverAddresses.size,
      s"There must be an equal number of heartbeat addresses and server " +
        s"addresses, but there are ${serverAddresses.size} servers and " +
        s"${heartbeatAddresses.size}."
    )
  }
}
