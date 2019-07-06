package frankenpaxos.echo

import collection.mutable
import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import java.io.File
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

  private var id: Long = 0
  private val promises = mutable.Map[Long, Promise[Unit]]()

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    promises.get(reply.id) match {
      case Some(promise) =>
        promise.success(())
        promises -= reply.id
      case None =>
        logger.fatal(s"Received reply for unpending echo ${reply.id}.")
    }
  }

  def echoImpl(promise: Promise[Unit]): Unit = {
    server.send(BenchmarkServerInbound(id = id))
    promises(id) = promise
    id += 1
  }

  def echo(): Future[Unit] = {
    val promise = Promise[Unit]()
    transport.executionContext.execute(() => echoImpl(promise))
    promise.future
  }
}
