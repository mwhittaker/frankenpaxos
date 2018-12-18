package zeno.examples

import scala.collection.mutable
import scala.scalajs.js.annotation._
import zeno.Actor
import zeno.Logger
import zeno.ProtoSerializer
import zeno.TypedActorClient

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
    addresses: Set[Transport#Address],
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

  @JSExportAll
  case class LeaderlessFollower(
      noPingTimer: Transport#Timer
  ) extends LeaderElectionState

  @JSExportAll
  case class Follower(
      noPingTimer: Transport#Timer,
      leader: Transport#Address
  ) extends LeaderElectionState

  @JSExportAll
  case class Candidate(
      notEnoughVotesTimer: Transport#Timer,
      votes: Set[Transport#Address]
  ) extends LeaderElectionState

  @JSExportAll
  case class Leader(pingTimer: Transport#Timer) extends LeaderElectionState

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

  val nodes: Map[
    Transport#Address,
    TypedActorClient[Transport, LeaderElectionActor[Transport]]
  ] = {
    for (address <- addresses)
      yield
        (address -> typedActorClient[LeaderElectionActor[Transport]](
          address,
          LeaderElectionActor.serializer
        ))
  }.toMap

  var callbacks: mutable.Buffer[(Transport#Address) => Unit] = mutable.Buffer()
  var round: Int = 0
  var state: LeaderElectionState = {
    val t = noPingTimer()
    t.start()
    LeaderlessFollower(t)
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
    // If we hear a ping from an earlier round, we ignore it.
    if (ping.round < round) {
      return
    }

    // If we hear from a leader in a larger round, then we immediately become a
    // follower of that leader.
    if (ping.round > round) {
      transitionToFollower(ping.round, src)
      return
    }

    state match {
      case LeaderlessFollower(noPingTimer) => {
        transitionToFollower(ping.round, src)
      }
      case Follower(noPingTimer, leader) => {
        noPingTimer.reset()
      }
      case Candidate(notEnoughVotesTimer, votes) => {
        transitionToFollower(ping.round, src)
      }
      case Leader(pingTimer) => {
        // We are the leader and received a ping from ourselves. We can just
        // ignore this ping.
        logger.check_eq(address, src)
      }
    }
  }

  private def handleVoteRequest(
      src: Transport#Address,
      voteRequest: VoteRequest
  ): Unit = {
    // If we hear a vote request from an earlier round, we ignore it.
    if (voteRequest.round < round) {
      return
    }

    // If we hear a vote request from a node in a later round, we immediately
    // become a leaderless follower and vote for that node.
    if (voteRequest.round > round) {
      stopTimer(state)
      round = voteRequest.round
      val t = noPingTimer()
      t.start()
      state = LeaderlessFollower(t)
      nodes(src).send(LeaderElectionInbound().withVote(Vote(round = round)))
      return
    }

    // Otherwise, the vote request is for our current round. In all cases, we
    // ignore the request.
    // vote for self
  }

  private def handleVote(src: Transport#Address, vote: Vote): Unit = {
    // If we hear a vote from an earlier round, we ignore it.
    if (vote.round < round) {
      return
    }

    // Hearing a vote from a future round is impossible! We can't hear a vote
    // in round `r` unless we send a vote request in round `r`. If we're not
    // yet in round `vote.round`, then we never sent a vote request in round
    // `vote.round`.
    if (vote.round > round) {
      logger.fatal(
        s"A node received a vote for round ${vote.round} but is only in " +
          s"round $round."
      )
      return
    }

    state match {
      case LeaderlessFollower(noPingTimer) => {
        // If we're a leaderless follower in this round, then we haven't yet
        // become a candidate in this round. If we haven't yet become a
        // candidate, we never sent a vote request, so we cannot receive a
        // vote.
        logger.fatal(
          s"A node received a vote in round ${vote.round} but is a " +
            "leaderless follower."
        )
        return
      }
      case Follower(noPingTimer, leader) => {
        // It is possible that we were a candidate in this round, then heard
        // from a leader in this round and stepped down to follower. In this
        // case, we simply ignore the vote.
      }
      case Candidate(notEnoughVotesTimer, votes) => {
        logger.info(s"Received vote from $src")
        val newState = Candidate(notEnoughVotesTimer, votes + src)
        state = newState

        // If we've received votes from a majority of the nodes, then we are
        // the leader for this round. `addresses.size / 2 + 1` is just a
        // formula for a majority.
        if (newState.votes.size >= (addresses.size / 2 + 1)) {
          stopTimer(state)
          val t = pingTimer()
          t.start()
          state = Leader(t)

          for (address <- addresses) {
            nodes(address).send(
              LeaderElectionInbound().withPing(Ping(round = round))
            )
          }
        }
      }
      case Leader(pingTimer) => {
        // It is possible that a candidate is elected leader and then later
        // receives some votes. We just ignore these votes.
      }
    }
  }

  private def stopTimer(state: LeaderElectionState): Unit = {
    state match {
      case LeaderlessFollower(noPingTimer)   => { noPingTimer.stop() }
      case Follower(noPingTimer, _)          => { noPingTimer.stop() }
      case Candidate(notEnoughVotesTimer, _) => { notEnoughVotesTimer.stop() }
      case Leader(pingTimer)                 => { pingTimer.stop() }
    }
  }

  private def transitionToFollower(
      newRound: Int,
      leader: Transport#Address
  ): Unit = {
    stopTimer(state)
    round = newRound
    val t = noPingTimer()
    t.start()
    state = Follower(t, leader)

    for (callback <- callbacks) {
      callback(leader)
    }
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def pingTimer(): Transport#Timer = {
    // We make `t` a lazy val to avoid the circular definition.
    lazy val t: Transport#Timer = timer(
      "pingTimer",
      options.pingPeriod,
      () => {
        for (address <- addresses) {
          nodes(address).send(
            LeaderElectionInbound().withPing(Ping(round = round))
          )
        }
        t.start()
      }
    )
    t
  }

  private def noPingTimer(): Transport#Timer = {
    timer(
      "noPingTimer",
      randomDuration(options.noPingTimeoutMin, options.noPingTimeoutMax),
      () => {
        state match {
          case LeaderlessFollower(noPingTimer) => {
            transitionToCandidate()
          }
          case Follower(noPingTimer, leader) => {
            transitionToCandidate()
          }
          case Candidate(notEnoughVotesTimer, votes) => {
            logger.fatal("A no ping timer was triggered for a candidate!")
          }
          case Leader(pingTimer) => {
            logger.fatal("A no ping timer was triggered for a leader!")
          }
        }
      }
    )
  }

  private def notEnoughVotesTimer(): Transport#Timer = {
    timer(
      "notEnoughVotes",
      randomDuration(
        options.notEnoughVotesTimeoutMin,
        options.notEnoughVotesTimeoutMax
      ),
      () => {
        state match {
          case LeaderlessFollower(noPingTimer) => {
            logger.fatal(
              "A not enough votes timer was triggered for a leaderless " +
                "follower!"
            )
          }
          case Follower(noPingTimer, leader) => {
            logger.fatal(
              "A not enough votes timer was triggered for a follower!"
            )
          }
          case Candidate(notEnoughVotesTimer, votes) => {
            transitionToCandidate()
          }
          case Leader(pingTimer) => {
            logger.fatal("A not enough votes timer was triggered for a leader!")
          }
        }
      }
    )
  }

  private def transitionToCandidate(): Unit = {
    stopTimer(state)
    round += 1
    val t = notEnoughVotesTimer()
    t.start()
    state = Candidate(t, Set())

    for (address <- addresses) {
      nodes(address).send(
        LeaderElectionInbound().withVoteRequest(VoteRequest(round = round))
      )
    }
  }

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
}