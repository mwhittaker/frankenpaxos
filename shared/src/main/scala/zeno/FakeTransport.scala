package zeno

import scala.collection.mutable.HashMap;
import scala.collection.mutable.Buffer;

case class FakeTransportAddress(address: String) extends zeno.Address

class FakeTransportTimer(
    name: String,
    delay: java.time.Duration,
    f: () => Unit,
    logger: Logger
) extends zeno.Timer {
  private var started = false

  override def name(): String = {
    name
  }

  def delay(): java.time.Duration = {
    delay
  }

  override def start(): Unit = {
    started = true
  }

  override def stop(): Unit = {
    started = false
  }

  def run() {
    if (!started) {
      logger.warn(
        s"Attempted to run timer $name, but this timer is not started."
      )
    } else {
      f()
    }
  }
}

class FakeTransport(logger: Logger) extends Transport[FakeTransport] {
  type Address = FakeTransportAddress
  type Timer = FakeTransportTimer

  case class BufferedMessage(
      src: FakeTransport#Address,
      bytes: Array[Byte]
  )
  val actors =
    new HashMap[FakeTransport#Address, Actor[FakeTransport]]()
  val buffers =
    new HashMap[FakeTransport#Address, Buffer[BufferedMessage]]()

  override def register(
      address: FakeTransport#Address,
      actor: Actor[FakeTransport]
  ): Unit = {
    if (actors.contains(address)) {
      logger.fatal(
        s"Attempting to register an actor with address $address, but this " +
          s"transport already has an actor bound to $address."
      )
    }
    actors.put(address, actor);
  }

  def send(
      src: FakeTransport#Address,
      dst: FakeTransport#Address,
      bytes: Array[Byte]
  ): Unit = {
    val bufferedMessage = BufferedMessage(src, bytes)
    buffers.get(dst) match {
      case Some(bufferedMessages) => bufferedMessages += bufferedMessage
      case None                   => buffers.put(dst, Buffer(bufferedMessage))
    }
  }

  def timer(
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): FakeTransport#Timer = {
    new FakeTransportTimer(name, delay, f, logger)
  }
}
