package frankenpaxos.unanimousbpaxos

import VertexIdHelpers.vertexIdOrdering
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.clienttable.ClientTable
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.util
import scala.collection.mutable
import scala.concurrent.Future
import scala.scalajs.js.annotation._

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class LeaderOptions(
    resendDependencyRequestsTimerPeriod: java.time.Duration,
    resendPhase1asTimerPeriod: java.time.Duration,
    resendPhase2asTimerPeriod: java.time.Duration,
    recoverVertexTimerMinPeriod: java.time.Duration,
    recoverVertexTimerMaxPeriod: java.time.Duration
)

@JSExportAll
object LeaderOptions {
  val default = LeaderOptions(
    resendDependencyRequestsTimerPeriod = java.time.Duration.ofSeconds(1),
    resendPhase1asTimerPeriod = java.time.Duration.ofSeconds(1),
    resendPhase2asTimerPeriod = java.time.Duration.ofSeconds(1),
    recoverVertexTimerMinPeriod = java.time.Duration.ofMillis(500),
    recoverVertexTimerMaxPeriod = java.time.Duration.ofMillis(1500)
  )
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val committedCommandsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_committed_commands_total")
    .help(
      "Total number of commands that were committed (with potential " +
        "duplicates)."
    )
    .register()

  val fastPathSuccessTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_fast_path_success_total")
    .help("Total number of times a leader took the fast path successfully.")
    .register()

  val fastPathFailTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_fast_path_fail_total")
    .help(
      "Total number of times a leader failed to take the fast path " +
        "successfully."
    )
    .register()

  val recoverWithNoVotesTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_recover_with_no_votes_total")
    .help(
      "Total number of times a leader recovering in phase1 got no votes from " +
        "acceptors (i.e. k == -1)."
    )
    .register()

  val recoverFromClassicRoundTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_recover_from_classic_round_total")
    .help(
      "Total number of times a leader recovering in phase1 recovered from " +
        "another classic round (i.e. k > 0)."
    )
    .register()

  val recoverCommandFromFastRoundTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_recover_command_from_fast_round_total")
    .help(
      "Total number of times a leader recovering in phase1 recovered from " +
        "round 0 and had a unanimous command."
    )
    .register()

  val recoverNoopFromFastRoundTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_recover_noop_from_fast_round_total")
    .help(
      "Total number of times a leader recovering in phase1 recovered from " +
        "round 0 and had to propose noop."
    )
    .register()

  val resendDependencyRequestsTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_resend_dependency_requests_total")
    .help("Total number of times the leader resent DependencyRequest messages.")
    .register()

  val resendPhase1asTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_resend_phase1a_total")
    .help("Total number of times the leader resent Phase1a messages.")
    .register()

  val resendPhase2asTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_resend_phase2a_total")
    .help("Total number of times the leader resent Phase2a messages.")
    .register()

  val recoverVertexTotal: Counter = collectors.counter
    .build()
    .name("unanimous_bpaxos_leader_recover_vertex_total")
    .help("Total number of times the leader recovered an vertex.")
    .register()

  val dependencyGraphNumVertices: Gauge = collectors.gauge
    .build()
    .name("unanimous_bpaxos_leader_dependency_graph_num_vertices")
    .help("The number of vertices in the dependency graph.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("unanimous_bpaxos_leader_dependencies")
    .help("The number of dependencies that a command has.")
    .register()
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer

  type Round = Int
  type ClientPseudonym = Int
  type AcceptorIndex = Int

  @JSExportAll
  sealed trait State[Transport <: frankenpaxos.Transport[Transport]]

  @JSExportAll
  case class Phase2Fast[Transport <: frankenpaxos.Transport[Transport]](
      command: Command,
      phase2bFasts: mutable.Map[AcceptorIndex, Phase2bFast],
      resendDependencyRequests: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class Phase1[Transport <: frankenpaxos.Transport[Transport]](
      // The current round.
      round: Round,
      // Phase 1b responses.
      phase1bs: mutable.Map[AcceptorIndex, Phase1b],
      // A timer to resend phase 1as.
      resendPhase1as: Transport#Timer
  ) extends State[Transport]

  @JSExportAll
  case class Phase2Classic[Transport <: frankenpaxos.Transport[Transport]](
      // The current round.
      round: Round,
      // The value we're trying to get chosen.
      value: Acceptor.VoteValue,
      // Phase 2b responses.
      phase2bClassics: mutable.Map[AcceptorIndex, Phase2bClassic],
      // A timer to resend phase 2as.
      resendPhase2as: Transport#Timer
  ) extends State[Transport]

  // TODO(mwhittaker): Decide whether we need a Committed entry.
  @JSExportAll
  case class Committed[Transport <: frankenpaxos.Transport[Transport]](
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId]
  ) extends State[Transport]
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    // Public for Javascript visualizations.
    val stateMachine: StateMachine,
    // Public for Javascript visualizations.
    val dependencyGraph: DependencyGraph[
      VertexId,
      Unit,
      util.FakeCompactSet[VertexId]
    ] = new JgraphtDependencyGraph(new util.FakeCompactSet[VertexId]()),
    options: LeaderOptions = LeaderOptions.default,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Leader._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and get our index.
  logger.check(config.valid())
  logger.check(config.leaderAddresses.contains(address))
  private val index = config.leaderAddresses.indexOf(address)

  // Channels to the dependency service nodes.
  private val depServiceNodes: Seq[Chan[DepServiceNode[Transport]]] =
    for (address <- config.depServiceNodeAddresses)
      yield chan[DepServiceNode[Transport]](address, DepServiceNode.serializer)

  // Channels to the other leaders.
  private val otherLeaders: Seq[Chan[Leader[Transport]]] =
    for (a <- config.leaderAddresses if a != address)
      yield chan[Leader[Transport]](a, Leader.serializer)

  // Channels to the acceptors.
  private val acceptors: Seq[Chan[Acceptor[Transport]]] =
    for (address <- config.acceptorAddresses)
      yield chan[Acceptor[Transport]](address, Acceptor.serializer)

  // The next available vertex id. When a leader receives a command, it assigns
  // it a vertex id using nextVertexId and then increments nextVertexId.
  @JSExport
  protected var nextVertexId: Int = 0

  // The state of each vertex that the leader knows about.
  val states = mutable.Map[VertexId, State[Transport]]()

  // The client table, which records the latest commands for each client.
  @JSExport
  protected val clientTable =
    new ClientTable[(Transport#Address, ClientPseudonym), Array[Byte]]()

  // If a leader commits a command in vertex A with a dependency on uncommitted
  // vertex B, then the leader sets a timer to recover vertex B. This prevents
  // a vertex from being forever stalled.
  @JSExport
  protected val recoverVertexTimers = mutable.Map[VertexId, Transport#Timer]()

  // Helpers ///////////////////////////////////////////////////////////////////
  def roundSystem(vertexId: VertexId): RoundSystem =
    new RoundSystem.RotatedRoundZeroFast(config.n, vertexId.leaderIndex)

  def stopTimers(vertexId: VertexId): Unit = {
    states.get(vertexId) match {
      case None =>
      case Some(state: Phase2Fast[Transport]) =>
        state.resendDependencyRequests.stop()
      case Some(state: Phase1[Transport]) =>
        state.resendPhase1as.stop()
      case Some(state: Phase2Classic[Transport]) =>
        state.resendPhase2as.stop()
      case Some(state: Committed[Transport]) =>
    }
  }

  // willBeCommitted(vertexId) returns true if the leader is in a state that
  // will eventually get vertexId chosen (assuming the network is nice).
  private def willBeCommitted(vertexId: VertexId): Boolean = {
    states.get(vertexId) match {
      case None =>
        // If the leader is not leading the vertex at all, it won't be chosen.
        false
      case Some(_: Phase2Fast[_]) =>
        // If the leader is in Phase2Fast, it's possible that nothing will ever
        // be chosen because a fast quorum of nodes is unavailable.
        false
      case Some(_: Phase1[_]) | Some(_: Phase2Classic[_]) =>
        // If the leader is on the classic path of Paxos, something will
        // eventually get chosen.
        true
      case Some(_: Committed[_]) =>
        // If something is already chosen, well, it's chosen.
        true
    }
  }

  private def recover(vertexId: VertexId, nackRound: Int): Unit = {
    // We'll start recovery in a round higher than any we've used before.
    val rs = roundSystem(vertexId)
    val currentRound = states.get(vertexId) match {
      case None                                  => 0
      case Some(state: Phase2Fast[Transport])    => 0
      case Some(state: Phase1[Transport])        => state.round
      case Some(state: Phase2Classic[Transport]) => state.round
      case Some(_: Committed[Transport])         =>
        // Nothing to recover.
        return
    }
    val round = rs.nextClassicRound(index, Math.max(nackRound, currentRound))

    // Stop any currently running timers.
    stopTimers(vertexId)

    // Send phase1s to acceptors.
    // TODO(mwhittaker): Implement thriftiness.
    val phase1a = Phase1a(vertexId = vertexId, round = round)
    acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))

    // Update our state.
    states(vertexId) = Phase1(
      round = round,
      phase1bs = mutable.Map[AcceptorIndex, Phase1b](),
      resendPhase1as = makeResendPhase1asTimer(phase1a)
    )

    // Stop the recovery timer for this vertex. At this point, something
    // will get committed.
    recoverVertexTimers.get(vertexId).foreach(_.stop())
    recoverVertexTimers -= vertexId
  }

  private def commit(
      vertexId: VertexId,
      commandOrNoop: CommandOrNoop,
      dependencies: Set[VertexId],
      informOthers: Boolean
  ): Unit = {
    metrics.committedCommandsTotal.inc()

    // Stop any currently running timers.
    stopTimers(vertexId)

    // Update the leader state.
    states(vertexId) = Committed(commandOrNoop, dependencies)

    // Notify the other replicas.
    if (informOthers) {
      for (leader <- otherLeaders) {
        leader.send(
          LeaderInbound().withCommit(
            Commit(vertexId = vertexId,
                   value = VoteValueProto(commandOrNoop = commandOrNoop,
                                          dependency = dependencies.toSeq))
          )
        )
      }
    }

    // Stop any recovery timer for the current vertex, and start recovery
    // timers for any uncommitted vertices on which we depend.
    recoverVertexTimers.get(vertexId).foreach(_.stop())
    recoverVertexTimers -= vertexId
    for {
      v <- dependencies
      if !willBeCommitted(v)
      if !recoverVertexTimers.contains(v)
    } {
      recoverVertexTimers(v) = makeRecoverVertexTimer(v)
    }

    // Execute commands.
    dependencyGraph.commit(vertexId,
                           (),
                           new util.FakeCompactSet[VertexId](dependencies))
    val executable: Seq[VertexId] = dependencyGraph.execute()
    metrics.dependencyGraphNumVertices.set(dependencyGraph.numVertices)

    for (v <- executable) {
      import CommandOrNoop.Value
      states.get(v) match {
        case None | Some(_: Phase2Fast[_]) | Some(_: Phase1[_]) |
            Some(_: Phase2Classic[_]) =>
          logger.fatal(
            s"Vertex $vertexId is ready for execution but the leader " +
              s"doesn't have a Committed entry for it."
          )

        case Some(committed: Committed[Transport]) => {
          committed.commandOrNoop.value match {
            case Value.Empty =>
              logger.fatal("Empty CommandOrNoop.")

            case Value.Noop(Noop()) =>
              // Noop.
              metrics.executedNoopsTotal.inc()

            case Value.Command(command: Command) =>
              val clientAddress = transport.addressSerializer.fromBytes(
                command.clientAddress.toByteArray
              )
              val clientIdentity = (clientAddress, command.clientPseudonym)
              clientTable.executed(clientIdentity, command.clientId) match {
                case ClientTable.Executed(_) =>
                  // Don't execute the same command twice.
                  metrics.repeatedCommandsTotal.inc()

                case ClientTable.NotExecuted =>
                  val output = stateMachine.run(command.command.toByteArray)
                  clientTable.execute(clientIdentity, command.clientId, output)
                  metrics.executedCommandsTotal.inc()

                  // The leader of the command vertex returns the response to
                  // the client. If the leader is dead, then the client will
                  // eventually re-send its request and some other replica will
                  // reply, either from its client log or by getting the
                  // command chosen in a new vertex.
                  if (index == v.leaderIndex) {
                    val client =
                      chan[Client[Transport]](clientAddress, Client.serializer)
                    client.send(
                      ClientInbound().withClientReply(
                        ClientReply(clientPseudonym = command.clientPseudonym,
                                    clientId = command.clientId,
                                    result = ByteString.copyFrom(output))
                      )
                    )
                  }
              }
          }
        }
      }
    }
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendDependencyRequestsTimer(
      dependencyRequest: DependencyRequest
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendDependencyRequests [${dependencyRequest.vertexId}]",
      options.resendDependencyRequestsTimerPeriod,
      () => {
        metrics.resendDependencyRequestsTotal.inc()
        for (depServiceNode <- depServiceNodes) {
          depServiceNode.send(
            DepServiceNodeInbound().withDependencyRequest(dependencyRequest)
          )
        }
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPhase1asTimer(
      phase1a: Phase1a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase1as [${phase1a.vertexId}]",
      options.resendPhase1asTimerPeriod,
      () => {
        metrics.resendPhase1asTotal.inc()
        acceptors.foreach(_.send(AcceptorInbound().withPhase1A(phase1a)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPhase2asTimer(
      phase2a: Phase2a
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPhase2as [${phase2a.vertexId}]",
      options.resendPhase2asTimerPeriod,
      () => {
        metrics.resendPhase2asTotal.inc()
        acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeRecoverVertexTimer(vertexId: VertexId): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"recoverVertex [$vertexId]",
      Util.randomDuration(options.recoverVertexTimerMinPeriod,
                          options.recoverVertexTimerMaxPeriod),
      () => {
        metrics.recoverVertexTotal.inc()
        // We shouldn't be recovering an vertex if we're already recovering it.
        logger.check(!willBeCommitted(vertexId))
        recover(vertexId, nackRound = -1)
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    import LeaderInbound.Request
    inbound.request match {
      case Request.ClientRequest(r)  => handleClientRequest(src, r)
      case Request.Phase2BFast(r)    => handlePhase2bFast(src, r)
      case Request.Phase1B(r)        => handlePhase1b(src, r)
      case Request.Phase2BClassic(r) => handlePhase2bClassic(src, r)
      case Request.Nack(r)           => handleNack(src, r)
      case Request.Commit(r)         => handleCommit(src, r)
      case Request.Empty => {
        logger.fatal("Empty LeaderInbound encountered.")
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      clientRequest: ClientRequest
  ): Unit = {
    metrics.requestsTotal.labels("ClientRequest").inc()

    // If we already have a response to this request in the client table, we
    // simply return it.
    val clientIdentity = (src, clientRequest.command.clientPseudonym)
    clientTable.executed(clientIdentity, clientRequest.command.clientId) match {
      case ClientTable.NotExecuted =>
      // Not executed yet, we'll have to get it chosen.

      case ClientTable.Executed(None) =>
        // Executed already but a stale command. We ignore this request.
        return

      case ClientTable.Executed(Some(output)) =>
        // Executed already and is the most recent command. We relay the
        // response to the client.
        val client = chan[Client[Transport]](src, Client.serializer)
        client.send(
          ClientInbound()
            .withClientReply(
              ClientReply(
                clientPseudonym = clientRequest.command.clientPseudonym,
                clientId = clientRequest.command.clientId,
                result = ByteString.copyFrom(output)
              )
            )
        )
        return
    }

    // Create a new vertex id for this command.
    val vertexId = VertexId(index, nextVertexId)
    nextVertexId += 1

    // Send a request to the dependency service.
    val dependencyRequest =
      DependencyRequest(vertexId = vertexId, command = clientRequest.command)
    for (depServiceNode <- depServiceNodes) {
      depServiceNode.send(
        DepServiceNodeInbound().withDependencyRequest(dependencyRequest)
      )
    }

    // Update our state.
    states(vertexId) = Phase2Fast(
      command = clientRequest.command,
      phase2bFasts = mutable.Map[AcceptorIndex, Phase2bFast](),
      resendDependencyRequests =
        makeResendDependencyRequestsTimer(dependencyRequest)
    )

    // Start a recovery timer for this vertex.
    recoverVertexTimers(vertexId) = makeRecoverVertexTimer(vertexId)
  }

  private def handlePhase2bFast(
      src: Transport#Address,
      phase2bFast: Phase2bFast
  ): Unit = {
    metrics.requestsTotal.labels("Phase2bFast").inc()

    states.get(phase2bFast.vertexId) match {
      case state @ (None | Some(_: Phase1[_]) | Some(_: Phase2Classic[_]) |
          Some(_: Committed[_])) =>
        logger.debug(
          s"Leader received Phase2bFast for vertex " +
            s"${phase2bFast.vertexId}, but is not in Phase2Fast. The state " +
            s"for this vertex is $state."
        )

      case Some(state: Phase2Fast[Transport]) =>
        // Wait until we have a fast quorum of responses.
        state.phase2bFasts(phase2bFast.acceptorId) = phase2bFast
        if (state.phase2bFasts.size < config.fastQuorumSize) {
          return
        }

        // Pull out the set of commands and dependencies.
        val commandSet: Set[CommandOrNoop] =
          state.phase2bFasts.values.map(_.voteValue.commandOrNoop).toSet
        val dependenciesSet: Set[Set[VertexId]] =
          state.phase2bFasts.values
            .map(_.voteValue.dependency.toSet)
            .toSet

        // Sanity check that all the commands agree.
        val commandOrNoop = CommandOrNoop().withCommand(state.command)
        commandSet.foreach(logger.checkEq(_, commandOrNoop))

        // If all of the dependencies are the same, we can take the fast path.
        // Otherwise, have to take the slow path.
        if (dependenciesSet.size == 1) {
          metrics.fastPathSuccessTotal.inc()
          commit(phase2bFast.vertexId,
                 commandOrNoop,
                 dependenciesSet.head,
                 informOthers = true)
        } else {
          // If we can't take the fast path, then we need to bump our round and
          // try to get a value chosen on the slow path. Typically, we'd do the
          // full two rounds of Paxos. However, we can perform the coordinated
          // recovery optimization of Fast Paxos and skip phase 1. Thus, we
          // proceed directly to phase 2.
          metrics.fastPathFailTotal.inc()
          logger.checkEq(roundSystem(phase2bFast.vertexId).leader(1), index)
          val dependencies: Set[VertexId] = dependenciesSet.flatten
          val value = Acceptor.VoteValue(commandOrNoop = commandOrNoop,
                                         dependencies = dependencies)

          // Stop timers.
          state.resendDependencyRequests.stop()

          // Send Phase2s.
          val phase2a = Phase2a(vertexId = phase2bFast.vertexId,
                                round = 1,
                                voteValue = Acceptor.toProto(value))
          acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))

          // Update state.
          states(phase2bFast.vertexId) = Phase2Classic(
            round = 1,
            value = value,
            phase2bClassics = mutable.Map[AcceptorIndex, Phase2bClassic](),
            resendPhase2as = makeResendPhase2asTimer(phase2a)
          )

          // Stop the recovery timer for this vertex. At this point, something
          // will get committed.
          recoverVertexTimers.get(phase2bFast.vertexId).foreach(_.stop())
          recoverVertexTimers -= phase2bFast.vertexId
        }
    }
  }

  private def handlePhase1b(src: Transport#Address, phase1b: Phase1b): Unit = {
    metrics.requestsTotal.labels("Phase1b").inc()

    states.get(phase1b.vertexId) match {
      case state @ (None | Some(_: Phase2Fast[_]) | Some(_: Phase2Classic[_]) |
          Some(_: Committed[_])) =>
        logger.debug(
          s"Leader received a Phase1b for vertex ${phase1b.vertexId}, but is " +
            s"not currently in phase 1 for this vertex. The state is $state."
        )

      case Some(state: Phase1[Transport]) =>
        // Ignore phase1bs from old rounds.
        if (phase1b.round != state.round) {
          // We know that phase1b.round is less than state.round because if it
          // were higher, we would have received a Nack instead of a Phase1b.
          logger.checkLt(phase1b.round, state.round)
          logger.debug(
            s"Leader received a phase1b in round ${phase1b.round} in " +
              s"${phase1b.vertexId} but is in round ${phase1b.round}."
          )
          return
        }

        // Wait until we have a quorum of responses.
        state.phase1bs(phase1b.acceptorId) = phase1b
        if (state.phase1bs.size < config.classicQuorumSize) {
          return
        }

        // If we have a quorum of responses, then we can proceed as follows.
        // Let k be the largest vote round we received from an acceptor.
        //
        //   - If k == -1, then no acceptor has voted yet, and we are safe to
        //     propose a noop.
        //   - If k > 0, then there is a unique value in round k, and we must
        //     propose it.
        //   - If k == 0 and all acceptors voted for the same value v, then v
        //     may have been chosen, so we have to propose it.
        //   - If k == 0 and all not acceptors voted for the same value, then
        //     we are safe to propose a noop.
        val maxVoteRound = state.phase1bs.values.map(_.voteRound).max
        val proposal: Acceptor.VoteValue = if (maxVoteRound == -1) {
          metrics.recoverWithNoVotesTotal.inc()
          Acceptor.VoteValue(
            commandOrNoop = CommandOrNoop().withNoop(Noop()),
            dependencies = Set[VertexId]()
          )
        } else {
          val voteValues: Set[VoteValueProto] = state.phase1bs.values
            .filter((x: Phase1b) => x.voteRound == maxVoteRound)
            .map((x: Phase1b) => x.voteValue.get)
            .toSet
          if (maxVoteRound > 0) {
            metrics.recoverFromClassicRoundTotal.inc()
            logger.checkEq(voteValues.size, 1)
            Acceptor.fromProto(voteValues.head)
          } else if (voteValues.size == 1) {
            metrics.recoverCommandFromFastRoundTotal.inc()
            Acceptor.fromProto(voteValues.head)
          } else {
            metrics.recoverNoopFromFastRoundTotal.inc()
            Acceptor.VoteValue(
              commandOrNoop = CommandOrNoop().withNoop(Noop()),
              dependencies = Set[VertexId]()
            )
          }
        }

        // Send phase2as to the acceptors.
        // TODO(mwhittaker): Implement thriftiness.
        val phase2a = Phase2a(
          vertexId = phase1b.vertexId,
          round = state.round,
          voteValue = Acceptor.toProto(proposal)
        )
        acceptors.foreach(_.send(AcceptorInbound().withPhase2A(phase2a)))

        // Stop existing timers and update our state.
        state.resendPhase1as.stop()
        states(phase1b.vertexId) = Phase2Classic(
          round = state.round,
          value = proposal,
          phase2bClassics = mutable.Map[AcceptorIndex, Phase2bClassic](),
          resendPhase2as = makeResendPhase2asTimer(phase2a)
        )
    }
  }

  private def handlePhase2bClassic(
      src: Transport#Address,
      phase2bClassic: Phase2bClassic
  ): Unit = {
    metrics.requestsTotal.labels("Phase2bClassic").inc()

    states.get(phase2bClassic.vertexId) match {
      case state @ (None | Some(_: Phase2Fast[_]) | Some(_: Phase1[_]) |
          Some(_: Committed[_])) =>
        logger.debug(
          s"Leader received a Phase2bClassic for vertex " +
            s"${phase2bClassic.vertexId}, but is not currently in phase 2 " +
            s"for this vertex id. The state is $state."
        )

      case Some(state: Phase2Classic[Transport]) =>
        // Ignore phase2bs from old rounds.
        if (phase2bClassic.round != state.round) {
          // We know that phase2bClassic.round is less than state.round because
          // if it were higher, we would have received a Nack instead of a
          // Phase2b.
          logger.checkLt(phase2bClassic.round, state.round)
          logger.debug(
            s"Proposer received a phase2bClassic in round " +
              s"${phase2bClassic.round} in ${phase2bClassic.vertexId} but is " +
              s"in round ${phase2bClassic.round}."
          )
          return
        }

        // Wait until we have a quorum of responses.
        state.phase2bClassics(phase2bClassic.acceptorId) = phase2bClassic
        if (state.phase2bClassics.size < config.classicQuorumSize) {
          return
        }

        // Stop existing timers and update our state.
        commit(phase2bClassic.vertexId,
               commandOrNoop = state.value.commandOrNoop,
               dependencies = state.value.dependencies,
               informOthers = true)
    }
  }

  private def handleNack(
      src: Transport#Address,
      nack: Nack
  ): Unit = {
    metrics.requestsTotal.labels("Nack").inc()

    val round = states.get(nack.vertexId) match {
      case None =>
        logger.fatal(
          s"Leader received a Nack for vertex ${nack.vertexId}, but is not " +
            s"leading the vertex."
        )
      case Some(state: Phase2Fast[_])    => 0
      case Some(state: Phase1[_])        => state.round
      case Some(state: Phase2Classic[_]) => state.round
      case Some(state: Committed[_])     =>
        // Ignore nacks if we're already committed.
        return
    }

    if (nack.higherRound <= round) {
      logger.debug(
        s"Leader received a Nack in round ${nack.higherRound} but is " +
          s"already in round $round."
      )
      return
    }

    recover(nack.vertexId, nackRound = nack.higherRound)
  }

  private def handleCommit(
      src: Transport#Address,
      c: Commit
  ): Unit = {
    metrics.requestsTotal.labels("Commit").inc()
    commit(c.vertexId,
           c.value.commandOrNoop,
           c.value.dependency.toSet,
           informOthers = false)
  }
}
