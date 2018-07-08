package zeno

import scala.util.Try;

trait Transport[Self <: Transport[Self]] {
  type Address <: zeno.Address
  type Timer <: zeno.Timer

  def register(address: Self#Address, actor: Actor[Self]): Unit
  def timer(duration: java.time.Duration): Self#Timer
  def send(src: Self#Address, dst: Self#Address, msg: String): Unit
}
