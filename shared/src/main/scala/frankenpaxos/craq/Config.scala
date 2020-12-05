package frankenpaxos.craq

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    f: Int,
    chainNodeAddresses: Seq[Transport#Address]
) {
  val numChainNodes = chainNodeAddresses.size

  def checkValid(): Unit = {
    require(numChainNodes >= 2 * f + 1,
            s"Number of chain nodes must be >= 1. It's $f.")
  }
}
