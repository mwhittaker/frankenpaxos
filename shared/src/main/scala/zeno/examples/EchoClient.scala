package zeno.examples

import java.net.InetAddress
import java.net.InetSocketAddress
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
      server.send(EchoRequest(msg = "ping"));
      pingTimer.start()
    });

  var numMessagesReceived: Int = 0

  println(s"Echo client listening on $srcAddress.")
  pingTimer.start();

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    numMessagesReceived += 1
    logger.info(s"Received ${reply.msg} from $src.")
  }

  def echo(msg: String): Unit = {
    server.send(EchoRequest(msg = msg))
  }
}
