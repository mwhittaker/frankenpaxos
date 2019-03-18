package frankenpaxos.echo

import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._

@JSExportAll
object BenchmarkClientInboundSerializer
    extends ProtoSerializer[BenchmarkClientInbound] {
  type A = BenchmarkClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object BenchmarkClient {
  val serializer = BenchmarkClientInboundSerializer
}

@JSExportAll
class BenchmarkClient[Transport <: frankenpaxos.Transport[Transport]](
    srcAddress: Transport#Address,
    dstAddress: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(srcAddress, transport, logger) {
  override type InboundMessage = BenchmarkClientInbound
  override def serializer = BenchmarkClient.serializer

  private val server =
    chan[BenchmarkServer[Transport]](dstAddress, BenchmarkServer.serializer)

  private var id: Int = 0

  private var promise: Option[Promise[Unit]] = None

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    promise match {
      case Some(p) =>
        if (reply.id == id) {
          id += 1
          p.success(())
          promise = None
        }

      case None =>
      // Do nothing.
    }
  }

  private def _echo(msg: String, promise: Promise[Unit]): Unit = {
    this.promise match {
      case Some(p) =>
        promise.failure(new IllegalArgumentException())

      case None =>
        this.promise = Some(promise)
        server.send(BenchmarkServerInbound(id = id, msg = msg))
    }
  }

  def echo(msg: String): Future[Unit] = {
    val promise = Promise[Unit]()
    transport.executionContext.execute(() => _echo(msg, promise))
    promise.future
  }
}
