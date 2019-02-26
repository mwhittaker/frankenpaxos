// Often in a distributed system, one node wants to know whether another node
// is dead or alive. In general, a failure detector that detects failures
// perfectly is impossible, but we can implement a best-effort failure detector
// based on heartbeats.
//
// Here's the set up. We are given a set of nodes, and we run a heartbeat
// participant on every node. Every participant sends empty heartbeat pings to
// the other heartbeats. When a participant receives a heartbeat ping, it
// responds with a heartbeat pong. If a participant `a` sends pings to another
// participant `b` but does not receive pongs, then `a` concludes that `b` is
// dead.

package frankenpaxos.heartbeat

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import scala.scalajs.js.annotation._

// These options govern how the heartbeat protocol operates. These options
// mimic the keepalive interval, keepalive time, and keepalive retry options
// used to configureTCP keepalive.
case class HeartbeatOptions(
    // After a participant sends a ping, it waits `failPeriod` time for a pong.
    // If no pong comes, the participant sends another ping and again waits
    // `failPeriod` time.
    failPeriod: java.time.Duration,
    // If a heartbeat participant sends a ping and does receive a pong within
    // `failPeriod` time, then it waits `successPeriod` time to send another
    // ping.
    successPeriod: java.time.Duration,
    // If a participant has sent `numRetries` consecutive pings to some
    // participant `a` and not received a pong, then the participant deems `a`
    // as dead.
    numRetries: Int
)

@JSExportAll
object ParticipantInboundSerializer
    extends ProtoSerializer[ParticipantInbound] {
  type A = ParticipantInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Participant {
  val serializer = ParticipantInboundSerializer
}

object HeartbeatOptions {
  val default = HeartbeatOptions(
    failPeriod = java.time.Duration.ofSeconds(1),
    successPeriod = java.time.Duration.ofSeconds(10),
    numRetries = 5
  )
}

@JSExportAll
class Participant[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    addresses: Set[Transport#Address],
    options: HeartbeatOptions = HeartbeatOptions.default
) extends Actor(address, transport, logger) {
  override type InboundMessage = ParticipantInbound
  override val serializer = ParticipantInboundSerializer

  // Verify that this participant's address is in the set of addresses.
  logger.check(addresses.contains(address))

  @JSExport
  protected val otherAddresses: Set[Transport#Address] = addresses - address

  type ParticipantChan = Chan[Transport, Participant[Transport]]
  private val chans: Map[Transport#Address, ParticipantChan] = {
    for (a <- otherAddresses)
      yield a -> chan[Participant[Transport]](a, Participant.serializer)
  }.toMap

  // When a participant sents a heartbeat ping, it sets a fail timer. If it
  // doesn't hear back before the timer expires, it sends another. If it does,
  // it sets a success timer to send another ping.
  @JSExport
  protected val failTimers: Map[Transport#Address, Transport#Timer] = {
    for (a <- otherAddresses)
      yield a -> timer(s"failTimer$a", options.failPeriod, () => fail(a))
  }.toMap

  // Timers that are set after a participant receives a pong.
  @JSExport
  protected val successTimers: Map[Transport#Address, Transport#Timer] = {
    for (a <- otherAddresses)
      yield
        a -> timer(s"successTimer$a", options.successPeriod, () => succeed(a))
  }.toMap

  // The number of unacknowledged retries sent to every participant.
  @JSExport
  protected val numRetries: mutable.Map[Transport#Address, Int] = mutable.Map()
  for (a <- otherAddresses) {
    numRetries(a) = 0
  }

  // The addresses of participants that are alive.
  @JSExport
  protected val alive: mutable.Set[Transport#Address] =
    mutable.Set() ++ otherAddresses

  // Send a ping to every participant and start the timers.
  for ((a, chan) <- chans) {
    chan.send(ParticipantInbound().withPing(Ping()))
    failTimers(a).start()
  }

  override def receive(
      src: Transport#Address,
      inbound: InboundMessage
  ): Unit = {
    import ParticipantInbound.Request
    inbound.request match {
      case Request.Ping(r) => handlePing(src, r)
      case Request.Pong(r) => handlePong(src, r)
      case Request.Empty => {
        logger.fatal("Empty ParticipantInbound encountered.")
      }
    }
  }

  private def handlePing(src: Transport#Address, ping: Ping): Unit = {
    chans(src).send(ParticipantInbound().withPong(Pong()))
  }

  private def handlePong(src: Transport#Address, ping: Pong): Unit = {
    numRetries(src) = 0
    failTimers(src).stop()
    successTimers(src).start()
  }

  private def fail(a: Transport#Address): Unit = {
    numRetries(a) += 1
    chans(a).send(ParticipantInbound().withPing(Ping()))
    failTimers(a).start()
  }

  private def succeed(a: Transport#Address): Unit = {
    chans(a).send(ParticipantInbound().withPing(Ping()))
    failTimers(a).start()
  }

  // Returns the set of addresses that this participant thinks are alive. Note
  // that this method MUST only ever be called from an actor running on the
  // same transport.
  def unsafeAlive(): Set[Transport#Address] = Set() ++ alive
}
