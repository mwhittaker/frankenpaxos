package zeno.examples

import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer
import zeno.TypedActorClient

@JSExportAll
object EchoReplySerializer extends ProtoSerializer[EchoReply] {
  type A = EchoReply
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object EchoClientActor {
  val serializer = EchoReplySerializer
}

@JSExportAll
class EchoClientActor[Transport <: zeno.Transport[Transport]](
    srcAddress: Transport#Address,
    dstAddress: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(srcAddress, transport, logger) {
  override type InboundMessage = EchoReply
  override def serializer = EchoClientActor.serializer

  private val server = typedActorClient[EchoServerActor[Transport]](
    dstAddress,
    EchoServerActor.serializer
  )

  private val pingTimer: Transport#Timer =
    timer("pingTimer", java.time.Duration.ofSeconds(1), () => {
      println("CURRENT THREAD PING")
      println(Thread.currentThread().getId())

      _echo("ping")
      pingTimer.start()
    });
  pingTimer.start();

  var numMessagesReceived: Int = 0

  logger.info(s"Echo client listening on $srcAddress.")

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    println("CURRENT THREAD RECEIVE")
    println(Thread.currentThread().getId())
    numMessagesReceived += 1
    logger.info(s"Received ${reply.msg} from $src.")
  }

  private def _echo(msg: String): Unit = {
    println("CURRENT THREAD _ECHO")
    println(Thread.currentThread().getId())
    server.send(EchoRequest(msg = msg))
  }

  def echo(msg: String): Unit = {
    println("CURRENT THREAD ECHO")
    println(Thread.currentThread().getId())
    transport.executionContext().execute(() => _echo(msg))
  }
}
