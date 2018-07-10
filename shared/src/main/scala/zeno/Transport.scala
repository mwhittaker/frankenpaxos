package zeno

import scala.util.Try;

trait Transport[Self <: Transport[Self]] {
  type Address <: zeno.Address
  type Timer <: zeno.Timer

  def register(address: Self#Address, actor: Actor[Self]): Unit
  def send(src: Self#Address, dst: Self#Address, msg: String): Unit
  def timer(name: String, delay: java.time.Duration, f: () => Unit): Self#Timer
}
