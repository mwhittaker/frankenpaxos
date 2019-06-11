package frankenpaxos.simulator

import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.Transport
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
// message itself is stored in `bytes`. Note that bytes is a Vector instead of
// an array so that equality is checked structurally (i.e., two bytes are the
// same if they have the same contents, not if they are the exact same object).
case class FakeTransportMessage(
    src: FakeTransport#Address,
    dst: FakeTransport#Address,
    bytes: Vector[Byte]
)

class FakeTransport(logger: Logger) extends Transport[FakeTransport] {
  override type Address = FakeTransportAddress
  override val addressSerializer = FakeTransportAddressSerializer
  override type Timer = FakeTransportTimer

  val actors = mutable.Map[FakeTransport#Address, Actor[FakeTransport]]()
  val timers = mutable.Map[(FakeTransport#Address, String), Timer]()
  var messages = mutable.Buffer[FakeTransportMessage]()

  override def register(
      address: FakeTransport#Address,
      actor: Actor[FakeTransport]
  ): Unit = {
    if (actors.contains(address)) {
      logger.fatal(s"""Attempting to register an actor with address $address,
                      |but this transport already has an actor bound to
                      |$address.""".stripMargin.replaceAll("\n", " "))
    }
    actors(address) = actor
  }

  override def send(
      src: FakeTransport#Address,
      dst: FakeTransport#Address,
      bytes: Array[Byte]
  ): Unit = {
    messages += FakeTransportMessage(src, dst, bytes.to[Vector])
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
    timers
      .filter({ case (address_name, timer) => timer.running })
      .keySet
      .to[Set]
  }

  def deliverMessage(msg: FakeTransportMessage): Unit = {
    if (!messages.contains(msg)) {
      logger.warn(s"Attempted to deliver unsent message $msg.")
      return
    }
    messages -= msg

    actors.get(msg.dst) match {
      case Some(actor) =>
        actor.receiveImpl(msg.src, msg.bytes.to[Array])
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
  case class DeliverMessage(msg: FakeTransportMessage) extends Command
  case class TriggerTimer(address_and_name: (FakeTransportAddress, String))
      extends Command

  def generateCommand(fakeTransport: FakeTransport): Gen[Command] = {
    var subgens = mutable.Buffer[(Int, Gen[Command])]()

    if (fakeTransport.messages.size > 0) {
      subgens +=
        fakeTransport.messages.size ->
          Gen
            .oneOf(fakeTransport.messages)
            .map(DeliverMessage(_))
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
      case DeliverMessage(msg) =>
        fakeTransport.deliverMessage(msg)
      case TriggerTimer(address_and_name) =>
        fakeTransport.triggerTimer(address_and_name)
    }
  }
}
