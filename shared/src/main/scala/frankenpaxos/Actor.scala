package frankenpaxos

import scala.scalajs.js.annotation.JSExportAll

// TODO(mwhittaker): Document.
@JSExportAll
abstract class Actor[Transport <: frankenpaxos.Transport[Transport]](
    val address: Transport#Address,
    val transport: Transport,
    // TODO(mwhittaker): Remove logger. It's not needed.
    val logger: Logger
) {
  // Interface.
  type InboundMessage
  def serializer: Serializer[InboundMessage]
  def receive(src: Transport#Address, message: InboundMessage): Unit

  // Implementation.
  logger.info(s"Actor $this registering on address $address.")
  transport.register(address, this)

  type Chan[A <: frankenpaxos.Actor[Transport]] =
    frankenpaxos.Chan[Transport, A]

  def chan[A <: frankenpaxos.Actor[Transport]](
      dst: Transport#Address,
      serializer: Serializer[A#InboundMessage]
  ): Chan[A] = {
    new Chan[A](transport, address, dst, serializer)
  }

  // Sending and flushing.
  def send(dst: Transport#Address, bytes: Array[Byte]): Unit =
    transport.send(address, dst, bytes)
  def sendNoFlush(dst: Transport#Address, bytes: Array[Byte]): Unit =
    transport.sendNoFlush(address, dst, bytes)
  def flush(dst: Transport#Address): Unit = transport.flush(address, dst)

  // Creates a new Timer with the specified delay. The returned timer will not
  // be started; you have to start it yourself. Also note that name does not
  // uniquely identify a timer. Multiple timers can be created with the same
  // name.
  def timer(
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): Transport#Timer = {
    transport.timer(address, name, delay, f)
  }
}
