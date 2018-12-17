package zeno.examples

import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import scala.collection.mutable
import zeno.ProtoSerializer

@JSExportAll
object LeaderElectionInboundSerializer
    extends ProtoSerializer[LeaderElectionInbound] {
  type A = LeaderElectionInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object LeaderElectionActor {
  val serializer = LeaderElectionInboundSerializer
}

// When a node is voted the leader for a particular round, it begins sending
// pings to the other nodes. If a follower has not heard a ping from the leader
// in a sufficiently long time, it becomes a candidate and attempts to become a
// leader in a higher round. If a candidate has not received sufficiently many
// votes after a given timeout, it becomes a candidate in a higher round.
//
// Leaders send pings every `pingPeriod` seconds. Similarly, a follower will
// wait between `noPingTimeoutMin` and `noPingTimeoutMax` seconds after hearing
// a ping before becoming a candidate. The actual time waited is chosen
// uniformly at random to avoid collisions. Similarly, a candidate waits
// between `noVoteTimeoutMin` and `noVoteTimeoutMax` seconds to become a
// candidate at a higher round.
case class LeaderElectionOptions(
    pingPeriod: java.time.Duration,
    noPingTimeoutMin: java.time.Duration,
    noPingTimeoutMax: java.time.Duration,
    notEnoughVotesTimeoutMin: java.time.Duration,
    notEnoughVotesTimeoutMax: java.time.Duration
)

@JSExportAll
class LeaderElectionActor[Transport <: zeno.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    addresses: Seq[Transport#Address],
    options: LeaderElectionOptions = LeaderElectionOptions(
      pingPeriod = java.time.Duration.ofSeconds(1),
      noPingTimeoutMin = java.time.Duration.ofSeconds(5),
      noPingTimeoutMax = java.time.Duration.ofSeconds(10),
      notEnoughVotesTimeoutMin = java.time.Duration.ofSeconds(5),
      notEnoughVotesTimeoutMax = java.time.Duration.ofSeconds(10)
    )
) extends Actor(address, transport, logger) {
  // Possible states ///////////////////////////////////////////////////////////
  sealed trait LeaderElectionState

  case class LeaderlessFollower(
      noPingTimer: Transport#Timer,
      vote: Transport#Address
  ) extends LeaderElectionState

  case class Follower(
      noPingTimer: Transport#Timer,
      leader: Transport#Address
  ) extends LeaderElectionState

  case class Candidate(
      noVoteTimer: Transport#Timer,
      votes: Set[Transport#Address]
  ) extends LeaderElectionState

  case class Leader() extends LeaderElectionState

  // Members ///////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderElectionInbound
  override def serializer = LeaderElectionActor.serializer

  // Sanity check arguments.
  logger.check(addresses.contains(address))
  logger.check_le(options.noPingTimeoutMin, options.noPingTimeoutMax)
  logger.check_le(
    options.notEnoughVotesTimeoutMin,
    options.notEnoughVotesTimeoutMax
  )

  var callbacks: mutable.Buffer[(Transport#Address) => Unit] = mutable.Buffer()
  var round: Int = 0
  var state: LeaderElectionState = {
    val t = timer("noPingTimer", noPingTimeout(), noPingTimer)
    t.start()
    LeaderlessFollower(t, address)
  }

  // Callback registration /////////////////////////////////////////////////////
  def register(callback: (Transport#Address) => Unit) = {
    callbacks += callback
  }

  // Receive ///////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: InboundMessage
  ): Unit = {
    import LeaderElectionInbound.Request
    inbound.request match {
      case Request.Ping(r)        => handlePing(src, r)
      case Request.VoteRequest(r) => handleVoteRequest(src, r)
      case Request.Vote(r)        => handleVote(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handlePing(src: Transport#Address, ping: Ping): Unit = {
    ???
  }

  private def handleVoteRequest(
      src: Transport#Address,
      voteRequest: VoteRequest
  ): Unit = {
    ???
  }

  private def handleVote(src: Transport#Address, vote: Vote): Unit = {
    ???
  }

  // Timeouts //////////////////////////////////////////////////////////////////
  private def pingTimer(): Unit = { ??? }
  private def noPingTimer(): Unit = { ??? }
  private def notEnoughVotesTimer(): Unit = { ??? }

  // Helpers ///////////////////////////////////////////////////////////////////
  // Returns a duration sampled uniformly at random between min (inclusive) and
  // max (inclusive). For example,
  //
  //  randomDuration(
  //    java.time.Duration.ofSeconds(3),
  //    java.time.Duration.ofSeconds(5)
  //  )
  //
  // returns a random duration between 3 and 5 seconds.
  private def randomDuration(
      min: java.time.Duration,
      max: java.time.Duration
  ): java.time.Duration = {
    logger.check_le(min, max)
    val random = java.util.concurrent.ThreadLocalRandom.current()
    val delta = max.minus(min)
    min.plus(
      java.time.Duration.ofSeconds(
        random.nextLong(0, delta.getSeconds() + 1),
        random.nextInt(0, delta.getNano() + 1)
      )
    )
  }

  private def pingPeriod(): java.time.Duration = {
    options.pingPeriod
  }

  private def noPingTimeout(): java.time.Duration = {
    randomDuration(options.noPingTimeoutMin, options.noPingTimeoutMax)
  }

  private def notEnoughVotesTimeout(): java.time.Duration = {
    randomDuration(
      options.notEnoughVotesTimeoutMin,
      options.notEnoughVotesTimeoutMax
    )
  }
  // round = 0
  // state =
  // follower
  //   follower timeout
  // candidate of responses
  //   candidate timeout
  // leader
  //   leader timeout
  //
  // register callback on listen
}
