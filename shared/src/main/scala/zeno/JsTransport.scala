package zeno

import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._

@JSExportAll
case class JsTransportAddress(address: String) extends zeno.Address

@JSExportAll
class JsTransportTimer(
    val address: JsTransport#Address,
    // We don't name this parameter `name` because we don't want to override
    // the `name` method in zeno.Timer with the same name.
    val the_name: String,
    val delay: java.time.Duration,
    val f: () => Unit
) extends zeno.Timer {
  var running: Boolean = false

  override def name(): String = {
    the_name
  }

  override def start(): Unit = {
    running = true
  }

  override def stop(): Unit = {
    running = false
  }

  def run(): Unit = {
    running = false
    f()
  }

  def delayMilliseconds(): Long = {
    delay.toMillis()
  }
}

@JSExportAll
class JsTransport(logger: Logger) extends Transport[JsTransport] {
  type Address = JsTransportAddress
  type Timer = JsTransportTimer

  @JSExportAll
  case class Message(
      src: JsTransport#Address,
      dst: JsTransport#Address,
      bytes: Array[Byte]
  )

  val actors = new HashMap[JsTransport#Address, Actor[JsTransport]]()
  val timers = Buffer[JsTransport#Timer]()
  var bufferedMessages = Buffer[Message]()
  var stagedMessages = Buffer[Message]()

  def timersJs(): js.Array[JsTransport#Timer] = { timers.toJSArray }
  def bufferedMessagesJs(): js.Array[Message] = { bufferedMessages.toJSArray }
  def stagedMessagesJs(): js.Array[Message] = { stagedMessages.toJSArray }

  def timersForAddressJs(
      address: JsTransport#Address
  ): js.Array[JsTransport#Timer] = {
    timers.filter(_.address == address).toJSArray
  }

  def stagedMessagesForAddressJs(
      address: JsTransport#Address
  ): js.Array[Message] = {
    stagedMessages.filter(_.dst == address).toJSArray
  }

  override def register(
      address: JsTransport#Address,
      actor: Actor[JsTransport]
  ): Unit = {
    if (actors.contains(address)) {
      logger.fatal(
        s"Attempting to register an actor with address $address, but this " +
          s"transport already has an actor bound to $address."
      )
      return;
    }
    actors.put(address, actor);
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

  def stageMessage(msg: Message, check_exists: Boolean = true): Unit = {
    if (check_exists && !bufferedMessages.contains(msg)) {
      logger.fatal(
        s"Attempted to stage $msg, but that message was not buffered."
      );
      return;
    }

    if (check_exists) {
      bufferedMessages -= msg
    }
    stagedMessages += msg
  }

  def deliverMessage(msg: Message, check_exists: Boolean = true): Unit = {
    if (check_exists && !stagedMessages.contains(msg)) {
      logger.fatal(
        s"Attempted to deliver $msg, but that message was not staged."
      );
      return;
    }

    actors.get(msg.dst) match {
      case Some(actor) => {
        actor.receiveImpl(msg.src, msg.bytes)
      }
      case None =>
        logger.warn(
          s"Attempting to deliver a message to an actor at address " +
            s"${msg.dst}, but no actor is registered to this address."
        )
    }

    if (check_exists) {
      stagedMessages -= msg
    }
  }

  def dropMessage(msg: Message, check_exists: Boolean = true): Unit = {
    if (check_exists && !stagedMessages.contains(msg)) {
      logger.fatal(
        s"Attempted to drop $msg, but that message was not staged."
      );
      return;
    }

    stagedMessages -= msg
  }
}
