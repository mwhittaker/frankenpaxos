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
    logger: Logger,
    outputFilename: String
) extends Actor(srcAddress, transport, logger) {
  override type InboundMessage = BenchmarkClientInbound
  override def serializer = BenchmarkClient.serializer

  private val server =
    chan[BenchmarkServer[Transport]](dstAddress, BenchmarkServer.serializer)

  private val latencyWriter = CSVWriter.open(new File(outputFilename))
  latencyWriter.writeRow(Seq("address", "start", "stop", "latency_nanos"))

  private val startTimeNanos = mutable.Buffer[Long]()
  private val startTimes = mutable.Buffer[java.time.Instant]()

  startTimeNanos += System.nanoTime()
  startTimes += java.time.Instant.now()
  server.send(BenchmarkServerInbound(msg = "."))

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    val stopTimeNano = System.nanoTime()
    val stopTime = java.time.Instant.now()
    val startTimeNano = startTimeNanos.remove(0)
    val startTime = startTimes.remove(0)

    startTimeNanos += System.nanoTime()
    startTimes += java.time.Instant.now()
    server.send(BenchmarkServerInbound(msg = "."))

    latencyWriter.writeRow(
      Seq(
        srcAddress.toString(),
        startTime.toString(),
        stopTime.toString(),
        (stopTimeNano - startTimeNano).toString()
      )
    )
  }
}
