package zeno

import scala.scalajs.js.annotation._;
import io.netty.buffer.ByteBuf;

@JSExportAll
abstract class Actor[Transport <: zeno.Transport[Transport]](
    val address: Transport#Address,
    val transport: Transport
) {
  transport.register(address, this);

  def html(): String

  def receive(src: Transport#Address, msg: Array[Byte]): Unit

  def send(dst: Transport#Address, msg: Array[Byte]): Unit = {
    transport.send(address, dst, msg)
  }

  def timer(
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): Transport#Timer = {
    transport.timer(address, name, delay, f)
  }
}
