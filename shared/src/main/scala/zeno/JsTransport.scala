package zeno

import scala.collection.mutable.Buffer;
import scala.collection.mutable.HashMap;
import scala.collection.mutable.Map;
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._;

@JSExportAll
case class JsTransportAddress(address: String) extends zeno.Address

@JSExportAll
class JsTransportTimer(
    val address: JsTransport#Address,
    // We don't name this parameter name because we want to override the name
    // method in zeno.Timer with the same name.
    val the_name: String,
    val delay: java.time.Duration,
    val f: () => Unit
) extends zeno.Timer {
  var version: Int = 0
  var running: Boolean = false
  var cached_version: Int = 0
  var cached_running: Boolean = false

  override def name(): String = {
    the_name
  }

  override def start(): Unit = {
    if (!running) {
      version += 1
      running = true
    }
  }

  override def stop(): Unit = {
    if (running) {
      version += 1
      running = false
    }
  }

  def run(): Unit = {
    f()
  }

  def delayMilliseconds(): Long = {
    delay.toMillis()
  }

  def updateCache(): Unit = {
    cached_version = version
    cached_running = running
  }
}

@JSExportAll
class JsTransport(logger: Logger) extends Transport[JsTransport] {
  type Address = JsTransportAddress
  type Timer = JsTransportTimer

  val actors = new HashMap[JsTransport#Address, Actor[JsTransport]]()

  val timers = Buffer[JsTransport#Timer]()

  @JSExportAll
  case class Message(
      src: JsTransport#Address,
      dst: JsTransport#Address,
      bytes: Array[Byte]
  )

  val bufferedMessages = Buffer[Message]()

  override def register(
      address: JsTransport#Address,
      actor: Actor[JsTransport]
  ): Unit = {
    if (actors.contains(address)) {
      logger.fatal(
        s"Attempting to register an actor with address $address, but this " +
          s"transport already has an actor bound to $address."
      )
    }
    actors.put(address, actor);
  }

  def bufferedMessagesJs(): js.Array[Message] = {
    bufferedMessages.toJSArray
  }

  def clearBufferedMessages(): Unit = {
    bufferedMessages.clear()
  }

  def timersJs(): js.Array[JsTransport#Timer] = {
    timers.toJSArray
  }

  def deliverMessage(msg: Message): Unit = {
    actors.get(msg.dst) match {
      case Some(actor) => actor.receiveImpl(msg.src, msg.bytes)
      case None =>
        logger.warn(
          s"Attempting to deliver a message to an actor at address " +
            s"${msg.dst}, but no actor is registered to this address."
        )
    }
  }

  override def send(
      src: JsTransport#Address,
      dst: JsTransport#Address,
      bytes: Array[Byte]
  ): Unit = {
    bufferedMessages += Message(src, dst, bytes)
  }

  override def timer(
      address: JsTransport#Address,
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): JsTransport#Timer = {
    val timer = new JsTransportTimer(address, name, delay, f)
    timers += timer
    timer
  }

}
