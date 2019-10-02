// This file implements a basic leader election algorithm that is based off of
// Raft's leader election algorithm [1].
//
// Raft's leader election algorithm is divided into a sequence of numbered
// rounds. The algorithm guarantees that there is at most one leader per round.
// To accomplish this, the algorithm requires 2f+1 participants to tolerate f
// faults. Note that two participants can simultaneously think they are
// leaders, but just not in the same round.
//
// Our basic protocol does not guarantee at most one leader per round. Multiple
// participants can all think they are the leader of a particular round. As a
// result, we require only f+1 participants to tolerate f faults.
//
// At any given point in time, a participant is either a leader or a follower.
// A leader periodically pings all other nodes. A follower stays a follower so
// long as it receives pings from a leader. If a follower doesn't hear a ping
// for long enough, it becomes a leader in a larger round and starts sending
// pings. If a leader receives a ping from a different leader in a larger round
// (or the same round but with a larger leader), it becomes a follower.
// Randomized timeouts are used to make it unlikely that two participants
// forever duel to be a leader.
//
// [1]: https://raft.github.io/raft.pdf

package frankenpaxos.election.basic

import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import scala.collection.mutable
import scala.scalajs.js.annotation._

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

case class ElectionOptions(
    pingPeriod: java.time.Duration,
    noPingTimeoutMin: java.time.Duration,
    noPingTimeoutMax: java.time.Duration
)

object ElectionOptions {
  val default = ElectionOptions(
    pingPeriod = java.time.Duration.ofSeconds(30),
    noPingTimeoutMin = java.time.Duration.ofSeconds(60),
    noPingTimeoutMax = java.time.Duration.ofSeconds(120)
  )
}

@JSExportAll
class Participant[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    // The addresses of all participants.
    addresses: Seq[Transport#Address],
    // The index of the initial leader.
    initialLeaderIndex: Int = 0,
    options: ElectionOptions = ElectionOptions.default
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ParticipantInbound
  override def serializer = Participant.serializer

  @JSExportAll
  sealed trait State

  @JSExportAll
  case object Leader extends State

  @JSExportAll
  case object Follower extends State

  // Fields ////////////////////////////////////////////////////////////////////
  logger.check(addresses.contains(address))
  logger.checkLe(options.noPingTimeoutMin, options.noPingTimeoutMax)
  logger.checkLe(0, initialLeaderIndex)
  logger.checkLt(initialLeaderIndex, addresses.size)

  private val index: Int = addresses.indexOf(address)

  // The addresses of the _other_ participants.
  private val otherParticipants: Seq[Chan[Participant[Transport]]] =
    for (a <- addresses if a != address)
      yield chan[Participant[Transport]](a, Participant.serializer)

  // The callbacks to inform when a new leader is elected.
  private var callbacks: mutable.Buffer[(Int) => Unit] = mutable.Buffer()

  @JSExport
  protected var round: Int = 0

  @JSExport
  protected var leaderIndex: Int = initialLeaderIndex

  private lazy val pingTimer: Transport#Timer = timer(
    "pingTimer",
    options.pingPeriod,
    () => {
      ping(round = round, leaderIndex = index)
      pingTimer.start()
    }
  )

  private lazy val noPingTimer: Transport#Timer = timer(
    "noPingTimer",
    frankenpaxos.Util.randomDuration(options.noPingTimeoutMin,
                                     options.noPingTimeoutMax),
    () => {
      round += 1
      leaderIndex = index
      changeState(Leader)
    }
  )

  @JSExport
  protected var state: State =
    if (index == initialLeaderIndex) {
      pingTimer.start()
      Leader
    } else {
      noPingTimer.start()
      Follower
    }

  // Helpers ///////////////////////////////////////////////////////////////////
  def ping(round: Int, leaderIndex: Int): Unit = {
    otherParticipants.foreach(
      _.send(
        ParticipantInbound()
          .withPing(Ping(leaderIndex = leaderIndex, round = round))
      )
    )
  }

  def changeState(newState: State): Unit = {
    (state, newState) match {
      case (Leader, Leader)     => // Do nothing.
      case (Follower, Follower) => // Do nothing.
      case (Follower, Leader) =>
        noPingTimer.stop()
        pingTimer.start()
        state = Leader
        ping(round = round, leaderIndex = index)
        callbacks.foreach(c => c(leaderIndex))
      case (Leader, Follower) =>
        pingTimer.stop()
        noPingTimer.start()
        state = Follower
        callbacks.foreach(c => c(leaderIndex))
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: InboundMessage
  ): Unit = {
    import ParticipantInbound.Request
    inbound.request match {
      case Request.Ping(r) => handlePing(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handlePing(src: Transport#Address, ping: Ping): Unit = {
    val ordering = scala.math.Ordering.Tuple2[Int, Int]
    import ordering.mkOrderingOps

    val pingBallot = (ping.round, ping.leaderIndex)
    val ballot = (round, leaderIndex)
    state match {
      case Follower =>
        if (pingBallot < ballot) {
          logger.debug(
            s"A Participant received a stale Ping message in round " +
              s"${ping.round} with leader ${ping.leaderIndex} but is already " +
              s"in round $round with leader $leaderIndex. The Ping is being " +
              s"ignored."
          )
        } else if (pingBallot == ballot) {
          noPingTimer.reset()
        } else {
          round = ping.round
          leaderIndex = ping.leaderIndex
          noPingTimer.reset()
        }

      case Leader =>
        if (pingBallot <= ballot) {
          logger.debug(
            s"A Participant received a stale Ping message in round " +
              s"${ping.round} with leader ${ping.leaderIndex} but is already " +
              s"in round $round with leader $leaderIndex. The Ping is being " +
              s"ignored."
          )
        } else {
          round = ping.round
          leaderIndex = ping.leaderIndex
          changeState(Follower)
        }
    }
  }

  // API ///////////////////////////////////////////////////////////////////////
  def register(callback: (Int) => Unit) = {
    transport.executionContext.execute(() => { callbacks += callback })
  }
}
