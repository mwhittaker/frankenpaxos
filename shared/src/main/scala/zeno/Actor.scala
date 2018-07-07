package zeno

import scala.scalajs.js.annotation._;
import io.netty.buffer.ByteBuf;

@JSExportAll
abstract class Actor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport
) {
  transport.register(address, this);

  def html(): String

  def receive(src: Transport#Address, msg: String): Unit

  def timer(duration: java.time.Duration): Transport#Timer = {
    transport.timer(duration)
  }

  def send(dst: Transport#Address, msg: String): Unit = {
    transport.send(address, dst, msg)
  }
}
