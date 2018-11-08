package zeno

import io.netty.buffer.ByteBuf
import scala.scalajs.js.annotation._

@JSExportAll
abstract class Actor[Transport <: zeno.Transport[Transport]](
    val address: Transport#Address,
    val transport: Transport,
    val logger: Logger
) {
  type InboundMessage
  def parseInboundMessage(bytes: Array[Byte]): InboundMessage
  def parseInboundMessageToString(bytes: Array[Byte]): String
  def receive(src: Transport#Address, message: InboundMessage): Unit

  transport.register(address, this);

  def receiveImpl(src: Transport#Address, bytes: Array[Byte]): Unit = {
    receive(src, parseInboundMessage(bytes))
  }

  def send(dst: Transport#Address, bytes: Array[Byte]): Unit = {
    transport.send(address, dst, bytes)
  }

  def timer(
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): Transport#Timer = {
    transport.timer(address, name, delay, f)
  }
}
