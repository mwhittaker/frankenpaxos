package frankenpaxos

import scala.concurrent.ExecutionContext

trait Transport[Self <: Transport[Self]] {
  type Address <: frankenpaxos.Address
  type Timer <: frankenpaxos.Timer

  def register(
      address: Self#Address,
      actor: Actor[Self]
  ): Unit

  def send(src: Self#Address, dst: Self#Address, bytes: Array[Byte]): Unit

  def timer(
      address: Self#Address,
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): Self#Timer

  def executionContext(): ExecutionContext
}
