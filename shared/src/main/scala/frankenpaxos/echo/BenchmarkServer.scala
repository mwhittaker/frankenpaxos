package frankenpaxos.echo

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.scalajs.js.annotation._

@JSExportAll
object BenchmarkServerInboundSerializer
    extends ProtoSerializer[BenchmarkServerInbound] {
  type A = BenchmarkServerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

object BenchmarkServer {
  val serializer = BenchmarkServerInboundSerializer
}

@JSExportAll
class BenchmarkServer[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = BenchmarkServerInbound
  override def serializer = BenchmarkServer.serializer

  override def receive(
      src: Transport#Address,
      request: BenchmarkServerInbound
  ): Unit = {
    val client =
      chan[BenchmarkClient[Transport]](src, BenchmarkClient.serializer)
    client.send(BenchmarkClientInbound(id = request.id, msg = request.msg))
  }
}
