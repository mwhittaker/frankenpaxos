package frankenpaxos.batchedunreplicated

import scala.scalajs.js.annotation.JSExportAll

@JSExportAll
case class Config[Transport <: frankenpaxos.Transport[Transport]](
    batcherAddresses: Seq[Transport#Address],
    serverAddress: Transport#Address,
    proxyServerAddresses: Seq[Transport#Address]
)
