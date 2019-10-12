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
    // We don't name this parameter `name` because we don't want to override
    // the `name` method in frankenpaxos.Timer with the same name.
    val the_name: String,
    val id: Int,
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

  type TimerId = Int
  var timerId: TimerId = 0

  val actors = mutable.Map[FakeTransport#Address, Actor[FakeTransport]]()
  val timers = mutable.Map[TimerId, Timer]()
  var messages = mutable.Buffer[FakeTransportMessage]()

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
    actors(address) = actor
  }

  override def send(
      src: FakeTransport#Address,
      dst: FakeTransport#Address,
      bytes: Array[Byte]
  ): Unit = messages += FakeTransportMessage(src, dst, bytes.to[Vector])

  // FakeTransport does not do flushing, so sendNoFlush and send are the same.
  override def sendNoFlush(
      src: FakeTransport#Address,
      dst: FakeTransport#Address,
      bytes: Array[Byte]
  ): Unit = send(src, dst, bytes)

  override def flush(
      src: FakeTransport#Address,
      dst: FakeTransport#Address
  ): Unit = {
    // Do nothing.
  }

  override def timer(
      address: FakeTransport#Address,
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): FakeTransport#Timer = {
    val id = timerId
    timerId += 1

    val timer = new FakeTransportTimer(address, name, id, f)
    timers.put(id, timer)
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

  def deliverMessage(msg: FakeTransportMessage): Unit = {
    if (!messages.contains(msg)) {
      logger.warn(s"Attempted to deliver unsent message $msg.")
      return
    }
    messages -= msg

    actors.get(msg.dst) match {
      case Some(actor) =>
        actor.receive(msg.src, actor.serializer.fromBytes(msg.bytes.to[Array]))
      // actor.receiveImpl(msg.src, msg.bytes.to[Array])
      case None =>
        logger.warn(
          s"Attempted to deliver a message to an actor at address " +
            s"${msg.dst}, but no actor is registered to this address."
        )
    }
  }

  def triggerTimer(timerId: Int): Unit = {
    if (!timers.contains(timerId)) {
      logger.warn(
        s"Attempted to trigger timer $timerId, but no such timer is registered."
      )
      return
    }

    val timer = timers(timerId)
    if (!timer.running) {
      logger.warn(
        s"Attempted to trigger timer $timerId (${timer.address}, " +
          s"${timer.name()}), but this timer is not running."
      )
      return
    }

    timer.run()
  }

  def runningTimers(): Set[FakeTransport#Timer] =
    timers.values.filter(_.running).to[Set]
}

object FakeTransport {
  sealed trait Command
  case class DeliverMessage(msg: FakeTransportMessage) extends Command
  case class TriggerTimer(
      address: FakeTransportAddress,
      name: String,
      timerId: Int
  ) extends Command

  // Generate a FakeTransport command. Every possible command (i.e. delivering
  // a message or triggering a running timer) has equal probability.
  def generateCommand(fakeTransport: FakeTransport): Option[Gen[Command]] = {
    var subgens = mutable.Buffer[(Int, Gen[Command])]()

    if (fakeTransport.messages.size > 0) {
      subgens += fakeTransport.messages.size ->
        Gen.oneOf(fakeTransport.messages).map(DeliverMessage(_))
    }

    if (fakeTransport.runningTimers().size > 0) {
      subgens += fakeTransport.runningTimers().size ->
        Gen
          .oneOf(fakeTransport.runningTimers().to[Seq])
          .map(timer => {
            TriggerTimer(address = timer.address,
                         name = timer.name(),
                         timerId = timer.id)
          })
    }

    if (subgens.size > 0) {
      Some(Gen.frequency(subgens: _*))
    } else {
      None
    }
  }

  // generateCommandWithFrequency is useful when you want to decide between a
  // transport command and some other type of command using Gen.frequency.
  def generateCommandWithFrequency(
      fakeTransport: FakeTransport
  ): Option[(Int, Gen[Command])] = {
    val frequency =
      fakeTransport.messages.size + fakeTransport.runningTimers().size
    generateCommand(fakeTransport).map((frequency, _))
  }

  def runCommand(fakeTransport: FakeTransport, command: Command): Unit = {
    command match {
      case DeliverMessage(msg) =>
        fakeTransport.deliverMessage(msg)
      case TriggerTimer(address, name, timerId) =>
        fakeTransport.triggerTimer(timerId)
    }
  }
}
