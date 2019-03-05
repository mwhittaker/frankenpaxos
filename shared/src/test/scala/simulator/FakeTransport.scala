package frankenpaxos.simulator

import frankenpaxos._
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable
import scala.concurrent.ExecutionContext

case class FakeTransportAddress(address: String) extends frankenpaxos.Address

object FakeTransportAddressSerializer
    extends frankenpaxos.Serializer[FakeTransportAddress] {
  override def toBytes(x: FakeTransportAddress): Array[Byte] =
    x.address.getBytes()
  override def fromBytes(bytes: Array[Byte]): FakeTransportAddress =
    FakeTransportAddress(new String(bytes))
  override def toPrettyString(x: FakeTransportAddress): String =
    x.address
}

class FakeTransportTimer(
    val address: FakeTransport#Address,
    val the_name: String,
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
    running = false
    f()
  }
}

// A FakeTransportMessage represents a message sent from `src` to `dst`. The
// message itself is stored in `bytes`. The pretty printed version of the bytes
// are stored in `string`. Ideally, we would have a pretty printed version of
// every message. However, sometimes this is tricky. For example, if an actor
// sends a message to another actor that has not yet been registered, it's not
// clear what serializer to use to pretty print the string. Thus, we sometimes
// do not have a pretty printed version of a message.
//
// Note that bytes is an IndexedSeq instead of an array so that equality is
// checked structurally (i.e., two bytes are the same if they have the same
// contents, not if they are the exact same object).
//
// TODO(mwhittaker): Think about how to get the pretty printed version of every
// message.
case class FakeTransportMessage(
    src: FakeTransport#Address,
    dst: FakeTransport#Address,
    bytes: IndexedSeq[Byte],
    string: Option[String]
)

class FakeTransport(logger: Logger) extends Transport[FakeTransport] {
  override type Address = FakeTransportAddress
  override val addressSerializer = FakeTransportAddressSerializer
  override type Timer = FakeTransportTimer
  type Message = FakeTransportMessage

  val actors = mutable.HashMap[FakeTransport#Address, Actor[FakeTransport]]()
  val timers = mutable.HashMap[(FakeTransport#Address, String), Timer]()
  var messages = mutable.Buffer[FakeTransport#Message]()

  override def register(
      address: FakeTransport#Address,
      actor: Actor[FakeTransport]
  ): Unit = {
    if (actors.contains(address)) {
      logger.fatal(s"""Attempting to register an actor with address $address,
                      |but this transport already has an actor bound to
                      |$address.""".stripMargin.replaceAll("\n", " "))
    }

    actors.put(address, actor)
  }

  override def send(
      src: FakeTransport#Address,
      dst: FakeTransport#Address,
      bytes: Array[Byte]
  ): Unit = {
    if (actors.contains(dst)) {
      val dstActor = actors(dst)
      val serializer = dstActor.serializer
      val string = serializer.toPrettyString(serializer.fromBytes(bytes))
      messages += FakeTransportMessage(src,
                                       dst,
                                       bytes.to[IndexedSeq],
                                       Some(string))
    } else {
      messages += FakeTransportMessage(src, dst, bytes.to[IndexedSeq], None)
    }
  }

  override def timer(
      address: FakeTransport#Address,
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): FakeTransport#Timer = {
    if (timers.contains(address, name)) {
      logger.fatal(
        s"Attempted to register a timer named $name to an actor with address " +
          s"$address, but this actor already has a timer with this name."
      )
    }

    val timer = new FakeTransportTimer(address, name, f)
    timers.put((address, name), timer)
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

  def runningTimers(): Set[(FakeTransport#Address, String)] = {
    val running =
      for ((address_and_name, timer) <- timers if timer.running)
        yield (address_and_name, timer)
    running.keySet.to[Set]
  }

  def deliverMessage(msg: FakeTransport#Message): Unit = {
    if (!messages.contains(msg)) {
      logger.warn(s"Attempted to deliver unsent message $msg.")
      return
    }
    messages -= msg

    actors.get(msg.dst) match {
      case Some(actor) => {
        actor.receiveImpl(msg.src, msg.bytes.to[Array])
      }
      case None =>
        logger.warn(
          s"Attempted to deliver a message to an actor at address " +
            s"${msg.dst}, but no actor is registered to this address."
        )
    }
  }

  def triggerTimer(address_and_name: (FakeTransport#Address, String)): Unit = {
    if (!timers.contains(address_and_name)) {
      logger.warn(
        s"Attempted to trigger timer $address_and_name, but no such timer " +
          s"is registered."
      )
      return
    }

    val timer = timers(address_and_name)
    if (!timer.running) {
      logger.warn(
        s"Attempted to trigger timer $address_and_name, but this timer " +
          s"is not running."
      )
      return
    }

    timer.run()
  }
}

object FakeTransport {
  sealed trait Command

  case class DeliverMessage(msg: FakeTransport#Message) extends Command

  case class TriggerTimer(address_and_name: (FakeTransport#Address, String))
      extends Command

  def generateCommand(fakeTransport: FakeTransport): Gen[Command] = {
    var subgens = mutable.Buffer[(Int, Gen[Command])]()

    if (fakeTransport.messages.size > 0) {
      subgens += (
        (
          fakeTransport.messages.size,
          Gen
            .oneOf(fakeTransport.messages)
            .map(DeliverMessage(_))
        )
      )
    }

    if (fakeTransport.runningTimers().size > 0) {
      subgens += (
        (
          fakeTransport.runningTimers().size,
          Gen.oneOf(fakeTransport.runningTimers().to[Seq]).map(TriggerTimer(_))
        )
      )
    }

    Gen.frequency(subgens: _*)
  }

  def runCommand(fakeTransport: FakeTransport, command: Command): Unit = {
    command match {
      case DeliverMessage(msg) => fakeTransport.deliverMessage(msg)
      case TriggerTimer(address_and_name) =>
        fakeTransport.triggerTimer(address_and_name)
    }
  }
}
