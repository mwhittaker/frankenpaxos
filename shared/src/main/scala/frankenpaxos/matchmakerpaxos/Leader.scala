package frankenpaxos.matchmakerpaxos

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.election.basic.Participant
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.quorums.QuorumSystem
import frankenpaxos.quorums.QuorumSystemProto
import frankenpaxos.quorums.SimpleMajority
import frankenpaxos.quorums.UnanimousWrites
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}
@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer
}

@JSExportAll
case class LeaderOptions(
    measureLatencies: Boolean
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    measureLatencies = true
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakerpaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakerpaxos_leader_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors),
    seed: Long = System.currentTimeMillis()
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.leaderAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override val serializer = LeaderInboundSerializer
  type MatchmakerIndex = Int
  type AcceptorIndex = Int
  type Round = Int

  @JSExportAll
  sealed trait State

  @JSExportAll
  case object Inactive extends State

  @JSExportAll
  case class Matchmaking(
      v: String,
      quorumSystem: QuorumSystem[Int],
      matchReplies: mutable.Map[MatchmakerIndex, MatchReply]
  ) extends State

  @JSExportAll
  case class Phase1(
      v: String,
      quorumSystem: QuorumSystem[Int],
      previousQuorumSystems: Map[Round, QuorumSystem[Int]],
      acceptorToRounds: Map[AcceptorIndex, mutable.Set[Round]],
      pendingRounds: mutable.Set[Round],
      phase1bs: mutable.Map[AcceptorIndex, Phase1b]
  ) extends State

  @JSExportAll
  case class Phase2(
      v: String,
      quorumSystem: QuorumSystem[Int],
      phase2bs: mutable.Map[AcceptorIndex, Phase2b]
  ) extends State

  @JSExportAll
  case class Chosen(v: String) extends State

  // Fields ////////////////////////////////////////////////////////////////////
  // A random number generator instantiated from `seed`. This allows us to
  // perform deterministic randomized tests.
  private val rand = new Random(seed)

  private val index = config.leaderAddresses.indexOf(address)

  // Matchmaker channels.
  private val matchmakers: Seq[Chan[Matchmaker[Transport]]] =
    for (a <- config.matchmakerAddresses)
      yield chan[Matchmaker[Transport]](a, Matchmaker.serializer)

  // Acceptor channels.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (a <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](a, Acceptor.serializer)

  // For simplicity, we assume a round robin round system with leader 0 the
  // initial leader. In general, we can use any round system we'd like, but
  // round robin keeps things simple.
  private val roundSystem = new RoundSystem.ClassicRoundRobin(config.numLeaders)

  // If the leader is the active leader, then this is its round. If it is
  // inactive, then this is the largest active round it knows about.
  @JSExport
  protected var round: Round = -1

  // The leader's state. Public for testing.
  var state: State = Inactive

  // A list of the clients awaiting a response.
  private val clients = mutable.Buffer[Chan[Client[Transport]]]()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def timed[T](label: String)(e: => T): T = {
    if (options.measureLatencies) {
      val startNanos = System.nanoTime
      val x = e
      val stopNanos = System.nanoTime
      metrics.requestsLatency
        .labels(label)
        .observe((stopNanos - startNanos).toDouble / 1000000)
      x
    } else {
      e
    }
  }

  // Given acceptors a_0, ..., a_{n-1}, randomly create a quorum system.
  //
  // TODO(mwhittaker): For now, we select a quorum system at random because it
  // is the simplest thing to do. In full generality, we should pass in the
  // policy by which we choose quorums.
  private def getRandomQuorumSystem(
      n: Int
  ): (QuorumSystem[AcceptorIndex], QuorumSystemProto) = {
    val seed = rand.nextLong()

    // We randomly pick between simple majority quorums and unanimous write
    // quorums. The thing is, though, that we can only use simple majority
    // quorums if we have at least 2*f+1 acceptors.
    if (config.numAcceptors >= 2 * config.f + 1 && rand.nextBoolean()) {
      val quorumSystem = new SimpleMajority(
        rand
          .shuffle(List() ++ (0 until n))
          .take(2 * config.f + 1)
          .toSet,
        seed
      )
      (quorumSystem, QuorumSystem.toProto(quorumSystem))
    } else {
      val quorumSystem = new UnanimousWrites(
        rand.shuffle(List() ++ (0 until n)).take(config.quorumSize).toSet,
        seed
      )
      (quorumSystem, QuorumSystem.toProto(quorumSystem))
    }
  }

  // Start Phase 1 with the given round and value.
  private def startPhase1(newRound: Int, v: String): Unit = {
    round = newRound
    val (quorumSystem, quorumSystemProto) = getRandomQuorumSystem(
      config.numAcceptors
    )
    val matchRequest = MatchRequest(
      acceptorGroup =
        AcceptorGroup(round = round, quorumSystem = quorumSystemProto)
    )
    matchmakers.foreach(
      _.send(MatchmakerInbound().withMatchRequest(matchRequest))
    )

    // Update our state.
    state = Matchmaking(
      v = v,
      quorumSystem = quorumSystem,
      matchReplies = mutable.Map()
    )
  }

  private def handleNack(nackRound: Int): Unit = {
    if (nackRound <= round) {
      logger.debug(
        s"Leader received a nack in round $nackRound but is " +
          s"already in round $round. The nack is being ignored."
      )
      return
    }

    // TODO(mwhittaker): We should have sleeps here to avoid dueling.
    state match {
      case Inactive =>
        // Do nothing. We're not trying to get anything chosen anyway.
        ()
      case matchmaking: Matchmaking =>
        round =
          roundSystem.nextClassicRound(leaderIndex = index, round = nackRound)
        startPhase1(round, matchmaking.v)
      case phase1: Phase1 =>
        round =
          roundSystem.nextClassicRound(leaderIndex = index, round = nackRound)
        startPhase1(round, phase1.v)
      case phase2: Phase2 =>
        round =
          roundSystem.nextClassicRound(leaderIndex = index, round = nackRound)
        startPhase1(round, phase2.v)
      case chosen: Chosen =>
        // Do nothing. We've already chosen the value.
        ()
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import LeaderInbound.Request

    val label = inbound.request match {
      case Request.ClientRequest(_)  => "ClientRequest"
      case Request.MatchReply(_)     => "MatchReply"
      case Request.Phase1B(_)        => "Phase1b"
      case Request.Phase2B(_)        => "Phase2b"
      case Request.MatchmakerNack(_) => "MatchmakerNack"
      case Request.AcceptorNack(_)   => "AcceptorNack"
      case Request.Empty =>
        logger.fatal("Empty LeaderInbound encountered.")
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r)  => handleClientRequest(src, r)
        case Request.MatchReply(r)     => handleMatchReply(src, r)
        case Request.Phase1B(r)        => handlePhase1b(src, r)
        case Request.Phase2B(r)        => handlePhase2b(src, r)
        case Request.MatchmakerNack(r) => handleMatchmakerNack(src, r)
        case Request.AcceptorNack(r)   => handleAcceptorNack(src, r)
        case Request.Empty =>
          logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    state match {
      case Inactive =>
        round = roundSystem.nextClassicRound(leaderIndex = index, round = round)
        startPhase1(round, clientRequest.v)
        clients += chan[Client[Transport]](src, Client.serializer)

      case matchmaking: Matchmaking =>
        // We do a leader change because clients force liveness. This isn't the
        // best way to enforce liveness (it kind of sticks, actually), but it's
        // the simplest.
        round = roundSystem.nextClassicRound(leaderIndex = index, round = round)
        startPhase1(round, clientRequest.v)
        clients += chan[Client[Transport]](src, Client.serializer)

      case phase1: Phase1 =>
        // We do a leader change because clients force liveness. This isn't the
        // best way to enforce liveness (it kind of sticks, actually), but it's
        // the simplest.
        round = roundSystem.nextClassicRound(leaderIndex = index, round = round)
        startPhase1(round, clientRequest.v)
        clients += chan[Client[Transport]](src, Client.serializer)

      case phase2: Phase2 =>
        // We do a leader change because clients force liveness. This isn't the
        // best way to enforce liveness (it kind of sticks, actually), but it's
        // the simplest.
        round = roundSystem.nextClassicRound(leaderIndex = index, round = round)
        startPhase1(round, clientRequest.v)
        clients += chan[Client[Transport]](src, Client.serializer)

      case chosen: Chosen =>
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound().withClientReply(ClientReply(chosen = chosen.v))
        )
    }
  }

  private def handleMatchReply(
      src: Transport#Address,
      matchReply: MatchReply
  ): Unit = {
    state match {
      case (Inactive | _: Phase1 | _: Phase2 | _: Chosen) =>
        logger.debug(
          s"Leader received a MatchReply but is not currently matchmaking. " +
            s"The MatchReply is being ignored. The leader's state is $state."
        )

      case matchmaking: Matchmaking =>
        // Ignore stale rounds.
        if (matchReply.round != round) {
          logger.debug(
            s"Leader received a MatchReply in round ${matchReply.round} but " +
              s"is already in round $round. The MatchReply is being ignored."
          )
          // We can't receive MatchReplies from the future.
          logger.checkLt(matchReply.round, round)
          return
        }

        // Wait until we have a quorum of responses.
        matchmaking.matchReplies(matchReply.matchmakerIndex) = matchReply
        if (matchmaking.matchReplies.size < config.quorumSize) {
          return
        }

        // Compute the following:
        //
        //   - pendingRounds: the set of all rounds for which some quorum
        //     system was returned. We need to intersect the quorum systems in
        //     these rounds.
        //   - previousQuorumSystems: the quorum system for every round in
        //     pendingRounds.
        //   - acceptorIndices: the union of a read quorum for every round in
        //     `pendingRounds`. These are the acceptors to which we send a
        //     Phase 1a message.
        //
        //     TODO(mwhittaker): We only need to send to enough acceptors to
        //     form a read quorum for every round in `pendingRounds`. I think
        //     this might be some complicated NP complete problem or something,
        //     so we just take a union of a read set from every quorum. There
        //     might be a better way to do this.
        //   - acceptorToRounds: an index mapping each acceptor's index to the
        //     set of rounds that it appears in. When we receive a Phase 1b
        //     from an acceptor, this index allows us to quickly figure out
        //     which rounds to update.
        //
        // For example, imagine a leader receives the following two responses
        // from the matchmakers with all quorums being unanimous write quorums:
        //
        //     0   1   2   3
        //   +---+---+---+---+
        //   |0,1|   |   |0,4|
        //   +---+---+---+---+
        //   +---+---+---+---+
        //   |   |2,3|   |0,4|
        //   +---+---+---+---+
        //
        // Then,
        //
        //   - pendingRounds = {0, 1, 3}
        //   - acceptorIndices = {0, 1, 2, 3, 4}
        //   - acceptorToRounds = {0->[0,3], 1->[0], 2->[1], 3->[1], 4->[3]}
        val pendingRounds = mutable.Set[Round]()
        val previousQuorumSystems = mutable.Map[Round, QuorumSystem[Int]]()
        val acceptorIndices = mutable.Set[AcceptorIndex]()
        val acceptorToRounds = mutable.Map[AcceptorIndex, mutable.Set[Round]]()
        for {
          reply <- matchmaking.matchReplies.values
          group <- reply.acceptorGroup
        } {
          pendingRounds += group.round
          val quorumSystem = QuorumSystem.fromProto(group.quorumSystem)
          previousQuorumSystems(group.round) = quorumSystem
          acceptorIndices ++= quorumSystem.randomReadQuorum()

          for (index <- quorumSystem.nodes) {
            acceptorToRounds
              .getOrElseUpdate(index, mutable.Set[Round]())
              .add(group.round)
          }
        }

        // If there are no pending rounds, then we're done already! We can skip
        // straight to phase 2. Otherwise, we have to go through phase 1.
        if (pendingRounds.isEmpty) {
          // Send Phase2as.
          for (index <- matchmaking.quorumSystem.randomWriteQuorum()) {
            acceptors(index).send(
              AcceptorInbound().withPhase2A(
                Phase2a(round = round, value = matchmaking.v)
              )
            )
          }

          // Update our state.
          state = Phase2(v = matchmaking.v,
                         quorumSystem = matchmaking.quorumSystem,
                         phase2bs = mutable.Map())
        } else {
          // Send phase1s to all acceptors.
          //
          // TODO(mwhittaker): Add thriftiness. Thriftiness is a bit more
          // complicated with Matchmakers since different acceptor groups can
          // overlap.
          for (index <- acceptorIndices) {
            val acceptor = acceptors(index)
            acceptor.send(AcceptorInbound().withPhase1A(Phase1a(round = round)))
          }

          // Update our state.
          state = Phase1(
            v = matchmaking.v,
            quorumSystem = matchmaking.quorumSystem,
            previousQuorumSystems = previousQuorumSystems.toMap,
            acceptorToRounds = acceptorToRounds.toMap,
            pendingRounds = pendingRounds,
            phase1bs = mutable.Map()
          )
        }
    }
  }

  private def handlePhase1b(
      src: Transport#Address,
      phase1b: Phase1b
  ): Unit = {
    state match {
      case (Inactive | _: Matchmaking | _: Phase2 | _: Chosen) =>
        logger.debug(
          s"Leader received a Phase1b but is not currently in Phase1. " +
            s"The Phase1b is being ignored. The leader's state is $state."
        )

      case phase1: Phase1 =>
        // Ignore stale rounds.
        if (phase1b.round != round) {
          logger.debug(
            s"Leader received a Phase1b in round ${phase1b.round} but " +
              s"is already in round $round. The Phase1b is being ignored."
          )
          // We can't receive Phase1bs from the future.
          logger.checkLt(phase1b.round, round)
          return
        }

        // Wait until we have a read quorum for every pending round.
        logger.checkGt(phase1.pendingRounds.size, 0)
        phase1.phase1bs(phase1b.acceptorIndex) = phase1b
        for (round <- phase1.acceptorToRounds(phase1b.acceptorIndex)) {
          if (phase1
                .previousQuorumSystems(round)
                .isSuperSetOfReadQuorum(phase1.phase1bs.keys.toSet)) {
            phase1.pendingRounds.remove(round)
          }
        }
        if (!phase1.pendingRounds.isEmpty) {
          return
        }

        // Compute a safe value.
        val votes = phase1.phase1bs.values.flatMap(_.vote)
        val v = if (votes.isEmpty) {
          phase1.v
        } else {
          votes.maxBy(_.voteRound).voteValue
        }

        // Send Phase2as.
        for (index <- phase1.quorumSystem.randomWriteQuorum()) {
          acceptors(index).send(
            AcceptorInbound().withPhase2A(Phase2a(round = round, value = v))
          )
        }

        // Update our state.
        state = Phase2(v = v,
                       quorumSystem = phase1.quorumSystem,
                       phase2bs = mutable.Map())
    }
  }

  private def handlePhase2b(
      src: Transport#Address,
      phase2b: Phase2b
  ): Unit = {
    state match {
      case (Inactive | _: Matchmaking | _: Phase1 | _: Chosen) =>
        logger.debug(
          s"Leader received a Phase2b but is not currently in Phase2. " +
            s"The Phase2b is being ignored. The leader's state is $state."
        )

      case phase2: Phase2 =>
        // Ignore stale rounds.
        if (phase2b.round != round) {
          logger.debug(
            s"Leader received a Phase2b in round ${phase2b.round} but " +
              s"is already in round $round. The Phase2b is being ignored."
          )
          // We can't receive Phase2bs from the future.
          logger.checkLt(phase2b.round, round)
          return
        }

        // Wait until we've heard from a write quorum.
        phase2.phase2bs(phase2b.acceptorIndex) = phase2b
        if (!phase2.quorumSystem.isWriteQuorum(phase2.phase2bs.keys.toSet)) {
          return
        }

        // Inform the clients the value has been chosen.
        clients.foreach(
          _.send(
            ClientInbound().withClientReply(ClientReply(chosen = phase2.v))
          )
        )

        // Update our state.
        state = Chosen(v = phase2.v)
    }
  }

  private def handleMatchmakerNack(
      src: Transport#Address,
      nack: MatchmakerNack
  ): Unit = {
    handleNack(nack.round)
  }

  private def handleAcceptorNack(
      src: Transport#Address,
      nack: AcceptorNack
  ): Unit = {
    handleNack(nack.round)
  }
}
