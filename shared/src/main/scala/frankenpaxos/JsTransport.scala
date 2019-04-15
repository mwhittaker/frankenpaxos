package frankenpaxos

import collection.mutable
import scala.concurrent.ExecutionContext
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation._

@JSExportAll
case class JsTransportAddress(address: String) extends frankenpaxos.Address

@JSExportAll
object JsTransportAddressSerializer extends Serializer[JsTransportAddress] {
  override def toBytes(x: JsTransportAddress): Array[Byte] =
    x.address.getBytes()
  override def fromBytes(bytes: Array[Byte]): JsTransportAddress =
    JsTransportAddress(new String(bytes))
  override def toPrettyString(x: JsTransportAddress): String =
    x.address
}

@JSExportAll
class JsTransportTimer(
    val address: JsTransport#Address,
    // We don't name this parameter `name` because we don't want to override
    // the `name` method in frankenpaxos.Timer with the same name.
    val the_name: String,
    val delay: java.time.Duration,
    val f: () => Unit
) extends frankenpaxos.Timer {
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
    if (running) {
      running = false
      f()
    }
  }

  def delayMilliseconds(): Long = {
    delay.toMillis()
  }
}

@JSExportAll
case class JsTransportMessage(
    src: JsTransport#Address,
    dst: JsTransport#Address,
    bytes: Array[Byte],
    id: Int
)

@JSExportAll
class JsTransport(logger: Logger) extends Transport[JsTransport] {
  override type Address = JsTransportAddress
  override val addressSerializer = JsTransportAddressSerializer
  override type Timer = JsTransportTimer

  sealed trait Command
  case class DeliverMessage(msg: JsTransportMessage) extends Command
  case class TriggerTimer(address_and_name: (JsTransportAddress, String))
      extends Command

  val actors = mutable.Map[JsTransport#Address, Actor[JsTransport]]()
  var partitionedActors = Set[JsTransport#Address]()
  val timers = mutable.Buffer[JsTransport#Timer]()
  var bufferedMessages = mutable.Buffer[JsTransportMessage]()
  var stagedMessages = mutable.Buffer[JsTransportMessage]()
  val history = mutable.Buffer[Command]()

  var messageId: Int = 0

  def timersForAddress(
      address: JsTransport#Address
  ): mutable.Buffer[JsTransport#Timer] = {
    timers.filter(_.address == address)
  }

  def stagedMessagesForAddress(
      address: JsTransport#Address
  ): mutable.Buffer[JsTransportMessage] = {
    stagedMessages.filter(_.dst == address)
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
      return
    }
    actors(address) = actor
  }

  override def send(
      src: JsTransport#Address,
      dst: JsTransport#Address,
      bytes: Array[Byte]
  ): Unit = {
    if (!partitionedActors.contains(src) && !partitionedActors.contains(dst)) {
      bufferedMessages += JsTransportMessage(src, dst, bytes, messageId)
      messageId += 1
    }
  }

  override def timer(
      address: JsTransport#Address,
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): JsTransport#Timer = {
    // TODO(mwhittaker): If a timer already exists with the given name and it
    // is stopped, delete it. We are replacing that timer with a new one.
    val timer = new JsTransportTimer(address, name, delay, () => {
      history += TriggerTimer((address, name))
      f()
    })
    timers += timer
    timer
  }

  override def executionContext(): ExecutionContext = {
    // Yes, you shouldn't do this in general [1], but it's ok here.
    //
    // [1]: https://docs.scala-lang.org/overviews/core/futures.html
    new ExecutionContext {
      override def execute(runnable: Runnable): Unit = {
        runnable.run()
      }

      override def reportFailure(cause: Throwable): Unit = {
        cause.printStackTrace()
      }
    }
  }

  def stageMessage(msg: JsTransportMessage): Unit = {
    if (!bufferedMessages.contains(msg)) {
      logger.fatal(
        s"Attempted to stage $msg, but that message was not buffered."
      )
      return
    }

    bufferedMessages -= msg
    if (!partitionedActors.contains(msg.dst)) {
      stagedMessages += msg
    }
  }

  def deliverMessage(
      msg: JsTransportMessage,
      check_exists: Boolean = true
  ): Unit = {
    if (check_exists && !stagedMessages.contains(msg)) {
      logger.fatal(
        s"Attempted to deliver $msg, but that message was not staged."
      );
      return
    }

    actors.get(msg.dst) match {
      case Some(actor) => {
        actor.receiveImpl(msg.src, msg.bytes)
        history += DeliverMessage(msg)
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

  def dropMessage(
      msg: JsTransportMessage,
      check_exists: Boolean = true
  ): Unit = {
    if (check_exists && !stagedMessages.contains(msg)) {
      logger.fatal(
        s"Attempted to drop $msg, but that message was not staged."
      );
      return;
    }

    stagedMessages -= msg
  }

  def partitionActor(address: JsTransport#Address): Unit = {
    // Note that if we write `partitionedActors += address`, Vue won't detect
    // the change. We write the code like this so that Vue detects changes to
    // partitionedActors.
    partitionedActors = partitionedActors + address
  }

  def unpartitionActor(address: JsTransport#Address): Unit = {
    // Note that if we write `partitionedActors -= address`, Vue won't detect
    // the change. We write the code like this so that Vue detects changes to
    // partitionedActors.
    partitionedActors = partitionedActors - address
  }

  def commandToUnitTest(command: Command): String = {
    def enquote(s: String): String = "\"\"\"" + s + "\"\"\""

    def toFakeAddress(address: JsTransport#Address): String =
      s"""FakeTransportAddress("${address.address}")"""

    command match {
      case DeliverMessage(JsTransportMessage(src, dst, bytes, _)) =>
        val proto = actors(dst).serializer
          .fromBytes(bytes)
          .asInstanceOf[scalapb.GeneratedMessage]
        val protoName = proto.companion.scalaDescriptor.name
        val protoString = enquote(
          scalapb.TextFormat.printToSingleLineUnicodeString(proto)
        )
        s"transport.deliverMessage(" +
          s"FakeTransportMessage(" +
          s"src=${toFakeAddress(src)}, " +
          s"dst=${toFakeAddress(dst)}, " +
          s"bytes=$protoName.fromAscii($protoString).toByteArray.to[Vector]" +
          s")" +
          s")"

      case TriggerTimer((address, name)) =>
        s"""transport.triggerTimer((${toFakeAddress(address)}, "$name"))"""
    }
  }

  def unitTest(): Seq[String] = {
    // TODO(mwhittaker): Add ability for JS sim to add custom strings to
    // history (e.g., client.propose).
    Seq(
      "// The following unit test was autogenerated by the JsTransport. The ",
      "// unit test may be incomplete. Modify appropriately.",
      "// format: off"
    ) ++
      history.map(commandToUnitTest) ++
      Seq("// format: on")
  }
}
