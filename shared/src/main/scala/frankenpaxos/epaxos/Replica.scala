package frankenpaxos.epaxos

import BallotHelpers.BallotImplicits
import CommandOrNoopHelpers.CommandOrNoopImplicits
import InstanceHelpers.instanceOrdering
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Serializer
import frankenpaxos.Util
import frankenpaxos.clienttable.ClientTable
import frankenpaxos.compact.FakeCompactSet
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.thrifty.ThriftySystem
import frankenpaxos.util
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object ReplicaInboundSerializer extends ProtoSerializer[ReplicaInbound] {
  type A = ReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class ReplicaOptions(
    thriftySystem: ThriftySystem,
    resendPreAcceptsTimerPeriod: java.time.Duration,
    defaultToSlowPathTimerPeriod: java.time.Duration,
    resendAcceptsTimerPeriod: java.time.Duration,
    resendPreparesTimerPeriod: java.time.Duration,
    recoverInstanceTimerMinPeriod: java.time.Duration,
    recoverInstanceTimerMaxPeriod: java.time.Duration,
    // If `unsafeSkipGraphExecution` is true, replicas skip graph execution
    // entirely. Instead, they execute commands as soon as they are committed.
    //
    // As the name suggests, this is not safe. It completely breaks the
    // protocol. This flag should only be used to debug performance issues. For
    // example, disabling graph execution makes it easier to see if graph
    // execution is a bottleneck.
    //
    // Note that if unsafeSkipGraphExecution is true, then
    // executeGraphBatchSize and executeGraphTimerPeriod are completely
    // ignored.
    unsafeSkipGraphExecution: Boolean,
    // When a replica receives a committed command, it adds it to its
    // dependency graph. When executeGraphBatchSize commands have been
    // committed, the replica attempts to execute as many commands in the graph
    // as possible. If executeGraphBatchSize commands have not been committed
    // within executeGraphTimerPeriod since the last time the graph was
    // executed, the graph is executed.
    executeGraphBatchSize: Int,
    executeGraphTimerPeriod: java.time.Duration,
    // numBlockers argument to DependencyGraph#execute. -1 is None.
    numBlockers: Int,
    // If true, the replica records how long various things take to do and
    // reports them using the `epaxos_replica_requests_latency` metric.
    measureLatencies: Boolean,
    // If topKDependencies is equal to k for some k > 0, then a dependency
    // service node only returns the top-k dependencies for every leader.
    topKDependencies: Int,
    // If `unsafeReturnNoDependencies` is true, replicas return no dependencies
    // for every command. As the name suggests, this is unsafe and breaks the
    // protocol. It should be used only for performance debugging and
    // evaluation.
    unsafeReturnNoDependencies: Boolean
)

@JSExportAll
object ReplicaOptions {
  val default = ReplicaOptions(
    thriftySystem = ThriftySystem.NotThrifty,
    resendPreAcceptsTimerPeriod = java.time.Duration.ofSeconds(1),
    defaultToSlowPathTimerPeriod = java.time.Duration.ofSeconds(1),
    resendAcceptsTimerPeriod = java.time.Duration.ofSeconds(1),
    resendPreparesTimerPeriod = java.time.Duration.ofSeconds(1),
    recoverInstanceTimerMinPeriod = java.time.Duration.ofMillis(500),
    recoverInstanceTimerMaxPeriod = java.time.Duration.ofMillis(1500),
    unsafeSkipGraphExecution = false,
    executeGraphBatchSize = 1,
    executeGraphTimerPeriod = java.time.Duration.ofSeconds(1),
    numBlockers = -1,
    measureLatencies = true,
    topKDependencies = 1,
    unsafeReturnNoDependencies = false
  )
}

@JSExportAll
class ReplicaMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("epaxos_replica_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val executeGraphTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_execute_graph_total")
    .help("Total number of times the replica executed the dependency graph.")
    .register()

  val executeGraphTimerTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_execute_graph_timer_total")
    .help(
      "Total number of times the replica executed the dependency graph from " +
        "a timer."
    )
    .register()

  val executedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_executed_commands_total")
    .help("Total number of executed state machine commands.")
    .register()

  val executedNoopsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_executed_noops_total")
    .help("Total number of \"executed\" noops.")
    .register()

  val repeatedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_repeated_commands_total")
    .help("Total number of commands that were redundantly chosen.")
    .register()

  val committedCommandsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_committed_commands_total")
    .help(
      "Total number of commands that were committed (with potential " +
        "duplicates)."
    )
    .register()

  val preparePhasesStartedTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_prepare_phases_started_total")
    .labelNames("type") // "fast" or "classic".
    .help("Total number of prepare phases started.")
    .register()

  val resendPreAcceptsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_resend_pre_accepts_total")
    .help("Total number of times the leader resent PreAccept messages.")
    .register()

  val resendAcceptsTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_resend_accepts_total")
    .help("Total number of times the leader resent Accept messages.")
    .register()

  val resendPreparesTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_resend_prepares_total")
    .help("Total number of times the leader resent Prepare messages.")
    .register()

  val defaultToSlowPathTotal: Counter = collectors.counter
    .build()
    .name("epaxos_replica_default_to_slow_path_total")
    .help("Total number of times the leader defaulted to the slow path.")
    .register()

  val dependencyGraphNumVertices: Gauge = collectors.gauge
    .build()
    .name("epaxos_replica_dependency_graph_num_vertices")
    .help("The number of vertices in the dependency graph.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("epaxos_replica_dependencies")
    .help("The number of dependencies that a command has.")
    .register()

  val uncompactedDependencies: Summary = collectors.summary
    .build()
    .name("epaxos_replica_uncompacted_dependencies")
    .help("The number of uncompacted dependencies that a command has.")
    .register()

  val recoverInstanceTimers: Gauge = collectors.gauge
    .build()
    .name("epaxos_replica_recover_instance_timers")
    .help("The number of recover instance timers that a replica has.")
    .register()
}

// Note that there are a couple of types (e.g., CmdLogEntry, LeaderState) that
// you might expect to be in the Replica class instead of the Replica
// companion. Unfortunately, there is a technical reason related to scala.js
// that prevents us from doing so.
//
// If we place CmdLogEntry or LeaderState within the Replica class, then it
// becomes an inner class. When scala.js compiles an instance of an inner
// class, it includes a pointer to the enclosing object. For example, an
// instance of NoCommandEntry would end up looking something like this:
//
//   NoCommandEntry(
//       replica = ...,        // Something of type Replica.
//       ballot = Ballot(...),
//   )
//
// I don't know why scala.js does this. I don't even understand how it does
// this. Shouldn't you be able to instantiate a NoCommandEntry without an
// enclosing replica? Who knows.
//
// Anyway, when an object like this is put within a map within a Replica
// instance, and that map is displayed using <frankenpaxos-map>, then Vue does
// a deep watch on the map. The deep watch on the map watches the objects
// within the map, and since those objects have pointers to a Replica, Vue ends
// up watching every field inside Replica. This is especially bad because a
// Replica object includes a dependency graph which is cyclic. When Vue
// encounters cyclic data structures, it can overflow the stack and crash.
//
// To avoid these issues, we pull types out of the Replica class and put them
// into the companion Replica object. This prevents scala.js from embedding a
// parent pointer to a Replica, which prevents Vue from watching all fields
// within Replica, which prevents Vue from crashing. It's annoying and very
// subtle, but it's the best we can do for now.
@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer

  type ReplicaIndex = Int
  type ClientPseudonym = Int
  type ClientId = Int

  // The special null ballot. Replicas set their vote ballot to nullBallot to
  // indicate that they have not yet voted.
  val nullBallot = Ballot(-1, -1)

  // A command (or noop) along with its sequence number and dependencies. In
  // the EPaxos paper, these triples are denoted like this:
  //
  //                      (\gamma, seq_\gamma, deps_\gamma)
  @JSExportAll
  case class CommandTriple(
      commandOrNoop: CommandOrNoop,
      sequenceNumber: Int,
      dependencies: InstancePrefixSet
  )

  // The core data structure of every EPaxos replica, the cmd log, records
  // information about every instance that a replica knows about. In the EPaxos
  // paper, the cmd log is visualized as an infinite two-dimensional array that
  // looks like this:
  //
  //      ... ... ...
  //     |___|___|___|
  //   2 |   |   |   |
  //     |___|___|___|
  //   1 |   |   |   |
  //     |___|___|___|
  //   0 |   |   |   |
  //     |___|___|___|
  //       Q   R   S
  //
  // The array is indexed on the left by an instance number and on the bottom
  // by a replica id. Thus, every cell is indexed by an instance (e.g. Q.1),
  // and every cell contains the state of the instance that indexes it.
  // `CmdLogEntry` represents the data within a cell, and `cmdLog` represents
  // the cmd log.
  //
  // Note that EPaxos has a small bug in how it implements ballots. The EPaxos
  // TLA+ specification and Go implementation have a single ballot per command
  // log entry. As detailed in [1], this is a bug. We need two ballots, like
  // what is done in normal Paxos. Note that we haven't proven this two-ballot
  // implementation is correct, so it may also be wrong.
  //
  // [1]: https://drive.google.com/open?id=1dQ_cigMWJ7w9KAJeSYcH3cZoFpbraWxm
  @JSExportAll
  sealed trait CmdLogEntry

  // A NoCommandEntry represents a command entry for which a replica has not
  // yet received any command (i.e., hasn't yet received a PreAccept, Accept,
  // or Commit). This is possible, for example, when a replica receives a
  // Prepare for an instance it has previously not received any messages about.
  @JSExportAll
  case class NoCommandEntry(
      // ballot plays the role of a Paxos acceptor's ballot. voteBallot is absent
      // because it is always the null ballot.
      ballot: Ballot
  ) extends CmdLogEntry

  @JSExportAll
  case class PreAcceptedEntry(
      ballot: Ballot,
      voteBallot: Ballot,
      triple: CommandTriple
  ) extends CmdLogEntry

  @JSExportAll
  case class AcceptedEntry(
      ballot: Ballot,
      voteBallot: Ballot,
      triple: CommandTriple
  ) extends CmdLogEntry

  @JSExportAll
  case class CommittedEntry(
      // Note that ballots are missing from the committed entry because once a
      // command is committed, there is no need for them anymore.
      triple: CommandTriple
  ) extends CmdLogEntry

  // When a replica receives a command from a client, it becomes the leader of
  // the command, the designated replica that is responsible for driving the
  // protocol through its phases to get the command chosen. LeaderState
  // represents the state of a leader during various points in the lifecycle of
  // the protocol, whether the leader is pre-accepting, accepting, or preparing
  // (during recovery).
  sealed trait LeaderState[Transport <: frankenpaxos.Transport[Transport]]

  @JSExportAll
  case class PreAccepting[Transport <: frankenpaxos.Transport[Transport]](
      // Every EPaxos replica plays the role of a Paxos proposer _and_ a Paxos
      // acceptor. The ballot and voteBallot in a command log entry are used
      // when the replica acts like an acceptor. leaderBallots is used when a
      // replica acts like a leader. In particular, leaderBallots[instance] is
      // the ballot in which the replica is trying to get a value chosen. This
      // value is like the ballot stored by a Paxos proposer. Note that this
      // implementation of ballots differs from the one in EPaxos' TLA+ spec.
      ballot: Ballot,
      // The command being pre-accepted.
      commandOrNoop: CommandOrNoop,
      // PreAcceptOk responses, indexed by the replica that sent them.
      responses: mutable.Map[ReplicaIndex, PreAcceptOk],
      // If true, this command should avoid taking the fast path and resort
      // only to the slow path. In the normal case, avoid is false. During
      // recovery, avoid may sometimes be true.
      avoidFastPath: Boolean,
      // A timer to re-send PreAccepts.
      resendPreAcceptsTimer: Transport#Timer,
      // After a leader receives a classic quorum of responses, it waits a
      // certain amount of time for the fast path before reverting to the
      // classic path. This timer is used for that waiting.
      defaultToSlowPathTimer: Option[Transport#Timer]
  ) extends LeaderState[Transport]

  @JSExportAll
  case class Accepting[Transport <: frankenpaxos.Transport[Transport]](
      // See above.
      ballot: Ballot,
      // The command being accepted.
      triple: CommandTriple,
      // AcceptOk responses, indexed by the replica that sent them.
      responses: mutable.Map[ReplicaIndex, AcceptOk],
      // A timer to re-send Accepts.
      resendAcceptsTimer: Transport#Timer
  ) extends LeaderState[Transport]

  @JSExportAll
  case class Preparing[Transport <: frankenpaxos.Transport[Transport]](
      // See above.
      ballot: Ballot,
      // Prepare responses, indexed by the replica that sent them.
      responses: mutable.Map[ReplicaIndex, PrepareOk],
      // A timer to re-send Prepares.
      resendPreparesTimer: Transport#Timer
  ) extends LeaderState[Transport]
}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    // Public for the JS visualizations.
    val stateMachine: StateMachine,
    // An empty dependency graph. This is a constructor argument so that
    // ScalaGraphDependencyGraph can be passed in for the JS visualizations.
    // Public for the JS visualizations.
    val dependencyGraph: DependencyGraph[Instance, Int, InstancePrefixSet],
    @JSExport
    var options: ReplicaOptions = ReplicaOptions.default,
    metrics: ReplicaMetrics = new ReplicaMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  import Replica._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer
  type LeaderState = Replica.LeaderState[Transport]
  type PreAccepting = Replica.PreAccepting[Transport]
  type Accepting = Replica.Accepting[Transport]
  type Preparing = Replica.Preparing[Transport]

  // Fields ////////////////////////////////////////////////////////////////////
  logger.check(config.valid())
  logger.check(config.replicaAddresses.contains(address))
  private val index: ReplicaIndex = config.replicaAddresses.indexOf(address)

  private val me: Chan[Replica[Transport]] =
    chan[Replica[Transport]](address, Replica.serializer)

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (replicaAddress <- config.replicaAddresses)
      yield chan[Replica[Transport]](replicaAddress, Replica.serializer)

  private val otherReplicaAddresses: Seq[Transport#Address] =
    config.replicaAddresses.filter(_ != address)

  private val otherReplicas: Seq[Chan[Replica[Transport]]] =
    for (a <- otherReplicaAddresses)
      yield chan[Replica[Transport]](a, Replica.serializer)

  private val otherReplicasByAddress
    : Map[Transport#Address, Chan[Replica[Transport]]] = {
    for (a <- otherReplicaAddresses)
      yield a -> chan[Replica[Transport]](a, Replica.serializer)
  }.toMap

  // Public for testing.
  val cmdLog: mutable.Map[Instance, CmdLogEntry] =
    mutable.Map[Instance, CmdLogEntry]()

  // Every replica maintains a local instance number i, initially 0. When a
  // replica R receives a command, it assigns the command instance R.i and then
  // increments i. Thus, every replica fills in the cmd log vertically within
  // its column from bottom to top. `nextAvailableInstance` represents i.
  @JSExport
  protected var nextAvailableInstance: Int = 0

  // The default fast path ballot used by this replica.
  @JSExport
  protected val defaultBallot: Ballot = Ballot(0, index)

  // The largest ballot ever seen by this replica. largestBallot is used when a
  // replica receives a nack and needs to choose a larger ballot.
  @JSExport
  protected var largestBallot: Ballot = Ballot(0, index)

  // The number of committed commands that are in the graph that have not yet
  // been processed. We process the graph every `options.executeGraphBatchSize`
  // committed commands and every `options.executeGraphTimerPeriod` seconds. If
  // the timer expires, we clear this number.
  @JSExport
  protected var numPendingCommittedCommands: Int = 0

  // A timer to execute the dependency graph. If the batch size is 1 or if
  // graph execution is disabled, then there is no need for the timer.
  @JSExport
  protected val executeGraphTimer: Option[Transport#Timer] =
    if (options.executeGraphBatchSize == 1 ||
        options.unsafeSkipGraphExecution) {
      None
    } else {
      lazy val t: Transport#Timer = timer(
        "executeGraphTimer",
        options.executeGraphTimerPeriod,
        () => {
          metrics.executeGraphTimerTotal.inc()
          execute()
          numPendingCommittedCommands = 0
          t.start()
        }
      )
      t.start()
      Some(t)
    }

  // The client table records the response to the latest request from each
  // client. For example, if command c1 sends command x with id 2 to a leader
  // and the leader later executes x yielding response y, then c1 maps to (2,
  // y) in the client table.
  implicit private val addressSerializer = transport.addressSerializer
  @JSExport
  protected val clientTable =
    ClientTable[(Transport#Address, ClientPseudonym), Array[Byte]]()

  @JSExport
  protected val leaderStates = mutable.Map[Instance, LeaderState]()

  // An index used to efficiently compute dependencies.
  @JSExport
  protected val conflictIndex = stateMachine.topKConflictIndex[Instance](
    options.topKDependencies,
    config.replicaAddresses.size,
    InstanceHelpers.like
  )

  // If a replica commits a command in instance I with a dependency on
  // uncommitted instance J, then the replica sets a timer to recover instance
  // J. This prevents an instance from being forever stalled.
  @JSExport
  protected val recoverInstanceTimers = mutable.Map[Instance, Transport#Timer]()

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

  private def leaderBallot(leaderState: LeaderState): Ballot = {
    leaderState match {
      case state: PreAccepting => state.ballot
      case state: Accepting    => state.ballot
      case state: Preparing    => state.ballot
    }
  }

  private def instanceSequenceNumber(instance: Instance): Option[Int] = {
    cmdLog.get(instance) match {
      case None | Some(_: NoCommandEntry) => None
      case Some(entry: PreAcceptedEntry)  => Some(entry.triple.sequenceNumber)
      case Some(entry: AcceptedEntry)     => Some(entry.triple.sequenceNumber)
      case Some(entry: CommittedEntry)    => Some(entry.triple.sequenceNumber)
    }
  }

  private def isCommitted(instance: Instance): Boolean = {
    cmdLog.get(instance) match {
      case None | Some(_: NoCommandEntry) | Some(_: PreAcceptedEntry) |
          Some(_: AcceptedEntry) =>
        false
      case Some(_: CommittedEntry) => true
    }
  }

  private def thriftyOtherReplicas(n: Int): Set[Chan[Replica[Transport]]] = {
    // TODO(mwhittaker): Add heartbeats to real delays.
    val delays: Map[Transport#Address, java.time.Duration] = {
      for (a <- otherReplicaAddresses)
        yield a -> java.time.Duration.ofSeconds(0)
    }.toMap
    options.thriftySystem.choose(delays, n).map(otherReplicasByAddress(_))
  }

  // Note that in Basic EPaxos, we're supposed to compute sequence numbers.
  // However, when you're returning only the top k dependencies, it becomes
  // impossible to compute sequence numbers. They are not really needed though,
  // so we do without them.
  private def computeSequenceNumberAndDependencies(
      instance: Instance,
      commandOrNoop: CommandOrNoop
  ): (Int, InstancePrefixSet) = {
    import CommandOrNoop.Value
    val dependencies =
      commandOrNoop.value match {
        case Value.Command(command) =>
          val bytes = command.command.toByteArray
          if (options.topKDependencies == 1) {
            val dependencies = InstancePrefixSet.fromTopOne(
              conflictIndex.getTopOneConflicts(bytes)
            )
            dependencies.subtractOne(instance)
            dependencies
          } else {
            val dependencies =
              InstancePrefixSet.fromTopK(conflictIndex.getTopKConflicts(bytes))
            dependencies.subtractOne(instance)
            dependencies
          }

        case Value.Noop(noop) =>
          InstancePrefixSet(config.replicaAddresses.size)

        case Value.Empty =>
          logger.fatal("Empty CommandOrNoop.")
      }
    metrics.dependencies.observe(dependencies.size)
    metrics.uncompactedDependencies.observe(dependencies.uncompactedSize)
    (0, dependencies)
  }

  private def updateConflictIndex(
      instance: Instance,
      commandOrNoop: CommandOrNoop
  ): Unit = {
    commandOrNoop.bytes() match {
      case Some(bytes) => conflictIndex.put(instance, bytes)
      case None        =>
      // In Basic EPaxos here, we'd remove the instance from the conflict
      // index. However, fast conflict indexes don't remove easily. So, we'll
      // have more dependencies than strictly necessary, but that's okay.
    }
  }

  // stopTimers(instance) stops any timers that may be running in instance
  // `instance`. This is useful when we transition from one state to another
  // and need to make sure that all old timers have been stopped.
  private def stopTimers(instance: Instance): Unit = {
    leaderStates.get(instance) match {
      case None =>
      // No timers to stop.
      case Some(preAccepting: PreAccepting) =>
        preAccepting.resendPreAcceptsTimer.stop()
        preAccepting.defaultToSlowPathTimer.foreach(_.stop())
      case Some(accepting: Accepting) =>
        accepting.resendAcceptsTimer.stop()
      case Some(preparing: Preparing) =>
        preparing.resendPreparesTimer.stop()
    }
  }

  // Transition to the pre-accept phase.
  private def transitionToPreAcceptPhase(
      instance: Instance,
      ballot: Ballot,
      commandOrNoop: CommandOrNoop,
      avoidFastPath: Boolean
  ): Unit = {
    // Compute sequence number and dependencies.
    var (sequenceNumber, dependencies) =
      computeSequenceNumberAndDependencies(instance, commandOrNoop)

    // Update our command log. This part of the algorithm is a little subtle.
    //
    // This replica acts as both a leader of this instance (e.g., sending out
    // PreAccepts and receiving PreAcceptOks) and as an acceptor of this
    // instance (e.g., receiving PreAccepts and sending PreAcceptOks).
    //
    // leaderStates is state used by a replica when acting as a leader, and
    // cmdLog is state used by a replica when acting as an acceptor. We're
    // currently acting as a leader but are about to modify cmdLog. Typically
    // when we modify cmdLog, we first have to check a lot of things (e.g., our
    // ballot is big enough, we haven't already voted, etc.).
    //
    // Here, we don't do those checks. Instead, we know that if we're at this
    // point in the algorithm, then our current ballot is larger than any we've
    // ever seen. If we ever receive a message for this instance in a higher
    // ballot (and hence change the command log entry of this instance), we
    // stop being a leader.
    //
    // This subtlety and complexity is arguably a drawback of EPaxos.
    cmdLog.get(instance) match {
      case Some(_: CommittedEntry) =>
        logger.fatal(
          s"A replica is transitioning to the pre-accept phase for instance " +
            s"$instance, but this instance has already been committed."
        )

      case None =>
      // No checks to do.

      case Some(noCommand: NoCommandEntry) =>
        logger.checkLe(noCommand.ballot, ballot)(BallotHelpers.Ordering)

      case Some(accepted: AcceptedEntry) =>
        logger.checkLe(accepted.ballot, ballot)(BallotHelpers.Ordering)
        logger.checkLe(accepted.voteBallot, ballot)(BallotHelpers.Ordering)

      case Some(preAccepted: PreAcceptedEntry) =>
        logger.checkLe(preAccepted.ballot, ballot)(BallotHelpers.Ordering)
        logger.checkLe(preAccepted.voteBallot, ballot)(BallotHelpers.Ordering)
    }

    cmdLog -= instance
    cmdLog(instance) = PreAcceptedEntry(
      ballot = ballot,
      voteBallot = ballot,
      triple = CommandTriple(
        commandOrNoop = commandOrNoop,
        sequenceNumber = sequenceNumber,
        dependencies = dependencies
      )
    )
    updateConflictIndex(instance, commandOrNoop)

    // Send PreAccept messages to other replicas in a fast quorum.
    val dependenciesProto = dependencies.toProto()
    val preAccept = PreAccept(
      instance = instance,
      ballot = ballot,
      commandOrNoop = commandOrNoop,
      sequenceNumber = sequenceNumber,
      dependencies = dependenciesProto
    )
    thriftyOtherReplicas(config.fastQuorumSize - 1)
      .foreach(_.send(ReplicaInbound().withPreAccept(preAccept)))

    // Stop existing timers.
    stopTimers(instance)

    // Update our leader state.
    leaderStates -= instance
    leaderStates(instance) = Replica.PreAccepting(
      ballot = ballot,
      commandOrNoop = commandOrNoop,
      responses = mutable.Map[ReplicaIndex, PreAcceptOk](
        index -> PreAcceptOk(
          instance = instance,
          ballot = ballot,
          replicaIndex = index,
          sequenceNumber = sequenceNumber,
          dependencies = dependenciesProto
        )
      ),
      avoidFastPath = avoidFastPath,
      resendPreAcceptsTimer = makeResendPreAcceptsTimer(preAccept),
      defaultToSlowPathTimer = None
    )
  }

  // Transition to the accept phase.
  private def transitionToAcceptPhase(
      instance: Instance,
      ballot: Ballot,
      triple: CommandTriple
  ): Unit = {
    // Update our command log. This part of the algorithm is a little subtle.
    // See transitionToPreAcceptPhase for details.
    cmdLog.get(instance) match {
      case Some(_: CommittedEntry) =>
        logger.fatal(
          s"A replica is transitioning to the accept phase for instance " +
            s"$instance, but this instance has already been committed."
        )

      case None =>
      // No checks to do.

      case Some(noCommand: NoCommandEntry) =>
        logger.checkLe(noCommand.ballot, ballot)(BallotHelpers.Ordering)

      case Some(accepted: AcceptedEntry) =>
        logger.checkLe(accepted.ballot, ballot)(BallotHelpers.Ordering)
        logger.checkLe(accepted.voteBallot, ballot)(BallotHelpers.Ordering)

      case Some(preAccepted: PreAcceptedEntry) =>
        logger.checkLe(preAccepted.ballot, ballot)(BallotHelpers.Ordering)
        logger.checkLe(preAccepted.voteBallot, ballot)(BallotHelpers.Ordering)
    }
    cmdLog -= instance
    cmdLog(instance) =
      AcceptedEntry(ballot = ballot, voteBallot = ballot, triple)
    updateConflictIndex(instance, triple.commandOrNoop)

    // Send out an accept message to other replicas.
    val accept = Accept(
      instance = instance,
      ballot = ballot,
      commandOrNoop = triple.commandOrNoop,
      sequenceNumber = triple.sequenceNumber,
      dependencies = triple.dependencies.toProto()
    )
    thriftyOtherReplicas(config.slowQuorumSize - 1)
      .foreach(_.send(ReplicaInbound().withAccept(accept)))

    // Stop existing timers.
    stopTimers(instance)

    // Update leader state.
    leaderStates -= instance
    leaderStates(instance) = Replica.Accepting(
      ballot = ballot,
      triple = triple,
      responses = mutable.Map[ReplicaIndex, AcceptOk](
        index -> AcceptOk(
          instance = instance,
          ballot = ballot,
          replicaIndex = index
        )
      ),
      resendAcceptsTimer = makeResendAcceptsTimer(accept)
    )
  }

  // Take the slow path during the pre-accept phase.
  private def preAcceptingSlowPath(
      instance: Instance,
      preAccepting: PreAccepting
  ): Unit = {
    // Compute the new dependencies and sequence numbers.
    logger.check(preAccepting.responses.size >= config.slowQuorumSize)
    val preAcceptOks: Set[PreAcceptOk] = preAccepting.responses.values.toSet
    val sequenceNumber: Int = preAcceptOks.map(_.sequenceNumber).max
    val dependencies = InstancePrefixSet(config.replicaAddresses.size)
    for (preAcceptOk <- preAcceptOks) {
      dependencies.addAll(InstancePrefixSet.fromProto(preAcceptOk.dependencies))
    }
    transitionToAcceptPhase(
      instance,
      preAccepting.ballot,
      CommandTriple(preAccepting.commandOrNoop, sequenceNumber, dependencies)
    )
  }

  private def commit(
      instance: Instance,
      triple: CommandTriple,
      informOthers: Boolean
  ): Unit = {
    metrics.committedCommandsTotal.inc()

    // Stop any currently running timers.
    stopTimers(instance)

    // Update the command log.
    cmdLog -= instance
    cmdLog(instance) = CommittedEntry(triple)
    updateConflictIndex(instance, triple.commandOrNoop)

    // Update the leader state.
    leaderStates -= instance

    // Notify the other replicas.
    if (informOthers) {
      for (replica <- otherReplicas) {
        replica.send(
          ReplicaInbound().withCommit(
            Commit(
              instance = instance,
              commandOrNoop = triple.commandOrNoop,
              sequenceNumber = triple.sequenceNumber,
              dependencies = triple.dependencies.toProto()
            )
          )
        )
      }
    }

    // Stop any recovery timer for the current instance.
    recoverInstanceTimers.get(instance) match {
      case Some(timer) =>
        timer.stop()
        recoverInstanceTimers -= instance
      case None =>
      // Do nothing.
    }

    // If we're skipping the graph, execute the command right away. Otherwise,
    // commit the command to the dependency graph and execute the graph if we
    // have sufficiently many commands pending execution.
    if (options.unsafeSkipGraphExecution) {
      executeCommand(instance, triple.commandOrNoop)
    } else {
      timed("commit/dependencyGraph.commit") {
        dependencyGraph.commit(instance,
                               triple.sequenceNumber,
                               triple.dependencies)
      }
      numPendingCommittedCommands += 1
      if (numPendingCommittedCommands % options.executeGraphBatchSize == 0) {
        timed("commit/execute") { execute() }
        numPendingCommittedCommands = 0
        executeGraphTimer.foreach(_.reset())
      }
    }
  }

  private val numBlockers =
    if (options.numBlockers == -1) None else Some(options.numBlockers)
  private val executables = mutable.Buffer[Instance]()
  private val blockers = mutable.Set[Instance]()
  private def execute(): Unit = {
    // TODO(mwhittaker): Pass numBlockers in as option.
    // TODO(mwhittaker): Do something with blockers.
    timed("execute/dependencyGraph.appendExecute") {
      dependencyGraph.appendExecute(numBlockers, executables, blockers)
    }
    metrics.executeGraphTotal.inc()
    metrics.dependencyGraphNumVertices.set(dependencyGraph.numVertices)

    for (i <- blockers) {
      if (!recoverInstanceTimers.contains(i)) {
        recoverInstanceTimers(i) = makeRecoverInstanceTimer(i)
      }
    }
    metrics.recoverInstanceTimers.set(recoverInstanceTimers.size)

    for (i <- executables) {
      cmdLog.get(i) match {
        case None | Some(_: NoCommandEntry) | Some(_: PreAcceptedEntry) |
            Some(_: AcceptedEntry) =>
          logger.fatal(
            s"Instance $i is ready for execution but our command log " +
              s"doesn't have a CommittedEntry for it."
          )

        case Some(CommittedEntry(triple)) =>
          timed("execute/executeCommand") {
            executeCommand(i, triple.commandOrNoop)
          }
      }
    }

    // Clear the fields.
    executables.clear()
    blockers.clear()
  }

  private def executeCommand(
      instance: Instance,
      commandOrNoop: CommandOrNoop
  ): Unit = {
    import CommandOrNoop.Value

    commandOrNoop.value match {
      case Value.Empty =>
        logger.fatal("Empty CommandOrNoop.")

      case Value.Noop(Noop()) =>
        metrics.executedNoopsTotal.inc()

      case Value.Command(
          Command(clientAddressBytes, clientPseudonym, clientId, command)
          ) =>
        val clientAddress = transport.addressSerializer.fromBytes(
          clientAddressBytes.toByteArray
        )
        val clientIdentity = (clientAddress, clientPseudonym)
        clientTable.executed(clientIdentity, clientId) match {
          case ClientTable.Executed(_) =>
            // Don't execute the same command twice.
            metrics.repeatedCommandsTotal.inc()

          case ClientTable.NotExecuted =>
            val output = stateMachine.run(command.toByteArray)
            clientTable.execute(clientIdentity, clientId, output)
            metrics.executedCommandsTotal.inc()

            // The leader of the command instance returns the response to
            // the client. If the leader is dead, then the client will
            // eventually re-send its request and some other replica will
            // reply, either from its client log or by getting the
            // command chosen in a new instance.
            if (index == instance.replicaIndex) {
              val client =
                chan[Client[Transport]](clientAddress, Client.serializer)
              client.send(
                ClientInbound().withClientReply(
                  ClientReply(clientPseudonym = clientPseudonym,
                              clientId = clientId,
                              result = ByteString.copyFrom(output))
                )
              )
            }
        }
    }
  }

  private def transitionToPreparePhase(instance: Instance): Unit = {
    metrics.preparePhasesStartedTotal.inc()

    // Stop any currently running timers.
    stopTimers(instance)

    // Choose a ballot larger than any we've seen before.
    largestBallot = Ballot(largestBallot.ordering + 1, index)
    val ballot = largestBallot

    // Note that we don't touch the command log when we transition to the
    // prepare phase. We may have a command log entry in `instance`; we may
    // not.

    // Send Prepares to all replicas, including ourselves.
    val prepare = Prepare(instance = instance, ballot = ballot)
    (thriftyOtherReplicas(config.slowQuorumSize - 1) + me).foreach(
      _.send(ReplicaInbound().withPrepare(prepare))
    )

    // Update our leader state.
    leaderStates -= instance
    leaderStates(instance) = Replica.Preparing(
      ballot = ballot,
      responses = mutable.Map(),
      resendPreparesTimer = makeResendPreparesTimer(prepare)
    )
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPreAcceptsTimer(
      preAccept: PreAccept
  ): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPreAccepts ${preAccept.instance} ${preAccept.ballot}",
      options.resendPreAcceptsTimerPeriod,
      () => {
        metrics.resendPreAcceptsTotal.inc()
        otherReplicas.foreach(_.send(ReplicaInbound().withPreAccept(preAccept)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeDefaultToSlowPathTimer(
      instance: Instance
  ): Transport#Timer = {
    val t = timer(
      s"defaultToSlowPath ${instance}",
      options.defaultToSlowPathTimerPeriod,
      () => {
        metrics.defaultToSlowPathTotal.inc()
        leaderStates.get(instance) match {
          case None | Some(_: Accepting) | Some(_: Preparing) =>
            logger.fatal(
              "defaultToSlowPath timer triggered but replica is not " +
                "preAccepting."
            )
          case Some(preAccepting: PreAccepting) =>
            preAcceptingSlowPath(instance, preAccepting)
        }
      }
    )
    t.start()
    t
  }

  private def makeResendAcceptsTimer(accept: Accept): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendAccepts ${accept.instance} ${accept.ballot}",
      options.resendAcceptsTimerPeriod,
      () => {
        metrics.resendAcceptsTotal.inc()
        otherReplicas.foreach(_.send(ReplicaInbound().withAccept(accept)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeResendPreparesTimer(prepare: Prepare): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"resendPrepares ${prepare.instance} ${prepare.ballot}",
      options.resendPreparesTimerPeriod,
      () => {
        metrics.resendPreparesTotal.inc()
        replicas.foreach(_.send(ReplicaInbound().withPrepare(prepare)))
        t.start()
      }
    )
    t.start()
    t
  }

  private def makeRecoverInstanceTimer(instance: Instance): Transport#Timer = {
    lazy val t: Transport#Timer = timer(
      s"recoverInstance $instance",
      Util.randomDuration(options.recoverInstanceTimerMinPeriod,
                          options.recoverInstanceTimerMaxPeriod),
      () => {
        transitionToPreparePhase(instance)
        t.start()
      }
    )
    t.start()
    t
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    import ReplicaInbound.Request

    val label = inbound.request match {
      case Request.ClientRequest(_) => "ClientRequest"
      case Request.PreAccept(_)     => "PreAccept"
      case Request.PreAcceptOk(_)   => "PreAcceptOk"
      case Request.Accept(_)        => "Accept"
      case Request.AcceptOk(_)      => "AcceptOk"
      case Request.Commit(_)        => "Commit"
      case Request.Nack(_)          => "Nack"
      case Request.Prepare(_)       => "Prepare"
      case Request.PrepareOk(_)     => "PrepareOk"
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.ClientRequest(r) => handleClientRequest(src, r)
        case Request.PreAccept(r)     => handlePreAccept(src, r)
        case Request.PreAcceptOk(r)   => handlePreAcceptOk(src, r)
        case Request.Accept(r)        => handleAccept(src, r)
        case Request.AcceptOk(r)      => handleAcceptOk(src, r)
        case Request.Commit(r)        => handleCommit(src, r)
        case Request.Nack(r)          => handleNack(src, r)
        case Request.Prepare(r)       => handlePrepare(src, r)
        case Request.PrepareOk(r)     => handlePrepareOk(src, r)
        case Request.Empty => {
          logger.fatal("Empty ReplicaInbound encountered.")
        }
      }
    }
  }

  private def handleClientRequest(
      src: Transport#Address,
      request: ClientRequest
  ): Unit = {
    // If we already have a response to this request in the client table, we
    // simply return it.
    val clientIdentity = (src, request.command.clientPseudonym)
    clientTable.executed(clientIdentity, request.command.clientId) match {
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
              ClientReply(clientPseudonym = request.command.clientPseudonym,
                          clientId = request.command.clientId,
                          result = ByteString.copyFrom(output))
            )
        )
        return
    }

    val instance: Instance = Instance(index, nextAvailableInstance)
    nextAvailableInstance += 1
    transitionToPreAcceptPhase(instance,
                               defaultBallot,
                               CommandOrNoop().withCommand(request.command),
                               avoidFastPath = false)
  }

  private def handlePreAccept(
      src: Transport#Address,
      preAccept: PreAccept
  ): Unit = {
    // Make sure we should be processing this message at all. For example,
    // sometimes we nack it, sometimes we ignore it, sometimes we re-send
    // replies that we previously sent because of it.
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    val nack =
      ReplicaInbound().withNack(Nack(preAccept.instance, largestBallot))
    cmdLog.get(preAccept.instance) match {
      case None =>
      // We haven't seen anything for this instance yet, so we're good to
      // process the message.

      case Some(NoCommandEntry(ballot)) =>
        // Don't process messages from old ballots. Note that we want to have
        // `<` here instead of `<=` because we may have previously received a
        // Prepare in this ballot. Another way to think of it is that
        // pre-accepting is like phase 2 of Paxos, whereas preparing is like
        // phase 1. In phase 2, we don't reject a ballot if it is equal to our
        // ballot, only if it is smaller.
        if (preAccept.ballot < ballot) {
          replica.send(nack)
          return
        }

      case Some(PreAcceptedEntry(ballot, voteBallot, triple)) =>
        // Don't process messages from old ballots.
        if (preAccept.ballot < ballot) {
          replica.send(nack)
          return
        }

        // Ignore a PreAccept if we've already responded, but re-send our
        // response for liveness.
        if (preAccept.ballot == voteBallot) {
          replica.send(
            ReplicaInbound().withPreAcceptOk(
              PreAcceptOk(
                instance = preAccept.instance,
                ballot = preAccept.ballot,
                replicaIndex = index,
                sequenceNumber = triple.sequenceNumber,
                dependencies = triple.dependencies.toProto()
              )
            )
          )
          return
        }

      case Some(AcceptedEntry(ballot, voteBallot, _triple)) =>
        // Don't process messages from old ballots.
        if (preAccept.ballot < ballot) {
          replica.send(nack)
          return
        }

        // If we've already accepted in this ballot, we shouldn't be
        // PreAccepting it again.
        if (preAccept.ballot == voteBallot) {
          return
        }

      case Some(CommittedEntry(triple)) =>
        // The command has already been committed. No need to run the protocol.
        replica.send(
          ReplicaInbound().withCommit(
            Commit(instance = preAccept.instance,
                   commandOrNoop = triple.commandOrNoop,
                   sequenceNumber = triple.sequenceNumber,
                   dependencies = triple.dependencies.toProto())
          )
        )
        return
    }

    // If we're currently leading this instance and the ballot we just received
    // is larger than the ballot we're using, then we should stop leading the
    // instance and yield to the replica with the higher ballot.
    if (leaderStates.contains(preAccept.instance) &&
        preAccept.ballot > leaderBallot(leaderStates(preAccept.instance))) {
      stopTimers(preAccept.instance)
      leaderStates -= preAccept.instance
    }

    // Update largestBallot.
    largestBallot = BallotHelpers.max(largestBallot, preAccept.ballot)

    // Reset recovery timer.
    recoverInstanceTimers.get(preAccept.instance).foreach(_.reset())

    // Compute dependencies and sequence number.
    var (sequenceNumber, dependencies) = computeSequenceNumberAndDependencies(
      preAccept.instance,
      preAccept.commandOrNoop
    )
    sequenceNumber = Math.max(sequenceNumber, preAccept.sequenceNumber)
    dependencies.addAll(InstancePrefixSet.fromProto(preAccept.dependencies))

    // Update the command log.
    cmdLog.put(
      preAccept.instance,
      PreAcceptedEntry(
        ballot = preAccept.ballot,
        voteBallot = preAccept.ballot,
        triple = CommandTriple(
          commandOrNoop = preAccept.commandOrNoop,
          sequenceNumber = sequenceNumber,
          dependencies = dependencies
        )
      )
    )

    // Update the conflict index.
    updateConflictIndex(preAccept.instance, preAccept.commandOrNoop)

    // Send back our response.
    val leader = chan[Replica[Transport]](src, Replica.serializer)
    leader.send(
      ReplicaInbound().withPreAcceptOk(
        PreAcceptOk(
          instance = preAccept.instance,
          ballot = preAccept.ballot,
          replicaIndex = index,
          sequenceNumber = sequenceNumber,
          dependencies = dependencies.toProto()
        )
      )
    )
  }

  private def handlePreAcceptOk(
      src: Transport#Address,
      preAcceptOk: PreAcceptOk
  ): Unit = {
    leaderStates.get(preAcceptOk.instance) match {
      case None =>
        logger.debug(
          s"Replica received a PreAcceptOk in instance " +
            s"${preAcceptOk.instance} but is not leading the instance."
        )
        return

      case Some(_: Accepting) =>
        logger.debug(
          s"Replica received a PreAcceptOk in instance " +
            s"${preAcceptOk.instance} but is accepting."
        )
        return

      case Some(_: Preparing) =>
        logger.debug(
          s"Replica received a PreAcceptOk in instance " +
            s"${preAcceptOk.instance} but is preparing."
        )
        return

      case Some(
          preAccepting @ Replica.PreAccepting(ballot,
                                              commandOrNoop,
                                              responses,
                                              avoidFastPath,
                                              resendPreAcceptsTimer,
                                              defaultToSlowPathTimer)
          ) =>
        if (preAcceptOk.ballot != ballot) {
          logger.debug(
            s"Replica received a preAcceptOk in ballot " +
              s"${preAcceptOk.instance} but is currently leading ballot " +
              s"$ballot."
          )
          // If preAcceptOk.ballot were larger, then we would have received a
          // Nack instead of a PreAcceptOk.
          logger.checkLt(preAcceptOk.ballot, ballot)(BallotHelpers.Ordering)
          return
        }

        // Record the response. Note that we may have already received a
        // PreAcceptOk from this replica before.
        val oldNumberOfResponses = responses.size
        responses(preAcceptOk.replicaIndex) = preAcceptOk
        val newNumberOfResponses = responses.size

        // We haven't received enough responses yet. We still have to wait to
        // hear back from at least a quorum.
        if (newNumberOfResponses < config.slowQuorumSize) {
          return
        }

        // If we've achieved a classic quorum for the first time (and we're not
        // avoiding the fast path, and the classic quorum size is smaller than
        // the fast quorum size), we still want to wait for a fast quorum, but
        // we need to set a timer to default to taking the slow path.
        if (!avoidFastPath &&
            oldNumberOfResponses < config.slowQuorumSize &&
            newNumberOfResponses >= config.slowQuorumSize &&
            config.slowQuorumSize < config.fastQuorumSize) {
          logger.check(defaultToSlowPathTimer.isEmpty)
          leaderStates(preAcceptOk.instance) = preAccepting.copy(
            defaultToSlowPathTimer =
              Some(makeDefaultToSlowPathTimer(preAcceptOk.instance))
          )

          return
        }

        // If we _are_ avoiding the fast path, then we can take the slow path
        // right away. There's no need to wait after we've received a quorum of
        // responses.
        if (avoidFastPath && newNumberOfResponses >= config.slowQuorumSize) {
          preAcceptingSlowPath(preAcceptOk.instance, preAccepting)
          return
        }

        // If we've received a fast quorum of responses, we can try to take the
        // fast path!
        if (newNumberOfResponses >= config.fastQuorumSize) {
          logger.check(!avoidFastPath)

          // We extract all (seq, deps) pairs in the PreAcceptOks, excluding our
          // own. If any appears n-2 times or more, we're good to take the fast
          // path.
          val seqDeps: Seq[(Int, InstancePrefixSet)] =
            timed("handlePreAcceptOk/collect respones") {
              responses
                .filterKeys(_ != index)
                .values
                .to[Seq]
                .map(
                  p =>
                    (p.sequenceNumber,
                     InstancePrefixSet.fromProto(p.dependencies))
                )
            }
          val candidates = timed("handlePreAcceptOk/popularItems") {
            Util.popularItems(seqDeps, config.fastQuorumSize - 1)
          }

          // If we have N-2 matching responses, then we can take the fast path
          // and transition directly into the commit phase. If we don't have
          // N-2 matching responses, then we take the slow path.
          if (candidates.size > 0) {
            logger.checkEq(candidates.size, 1)
            val (sequenceNumber, dependencies) = candidates.head
            timed("handlePreAcceptOk/commit") {
              commit(
                preAcceptOk.instance,
                CommandTriple(commandOrNoop, sequenceNumber, dependencies),
                informOthers = true
              )
            }
          } else {
            // There were not enough matching (seq, deps) pairs. We have to
            // resort to the slow path.
            preAcceptingSlowPath(preAcceptOk.instance, preAccepting)
          }
          return
        }
    }
  }

  private def handleAccept(src: Transport#Address, accept: Accept): Unit = {
    // Make sure we should be processing this message at all.
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    val nack = ReplicaInbound().withNack(Nack(accept.instance, largestBallot))
    cmdLog.get(accept.instance) match {
      case None =>
      // We haven't seen anything for this instance yet, so we're good to
      // process the message.

      case Some(NoCommandEntry(ballot)) =>
        // Don't process messages from old ballots.
        if (accept.ballot < ballot) {
          replica.send(nack)
          return
        }

      case Some(PreAcceptedEntry(ballot, voteBallot, triple)) =>
        // Don't process messages from old ballots.
        if (accept.ballot < ballot) {
          replica.send(nack)
          return
        }

      case Some(AcceptedEntry(ballot, voteBallot, _triple)) =>
        // Don't process messages from old ballots.
        if (accept.ballot < ballot) {
          replica.send(nack)
          return
        }

        // Ignore an Accept if we've already responded, but re-send our
        // response for liveness.
        if (accept.ballot == voteBallot) {
          replica.send(
            ReplicaInbound().withAcceptOk(
              AcceptOk(instance = accept.instance,
                       ballot = accept.ballot,
                       replicaIndex = index)
            )
          )
          return
        }

      case Some(CommittedEntry(triple)) =>
        // The command has already been committed. No need to run the protocol.
        replica.send(
          ReplicaInbound().withCommit(
            Commit(instance = accept.instance,
                   commandOrNoop = triple.commandOrNoop,
                   sequenceNumber = triple.sequenceNumber,
                   dependencies = triple.dependencies.toProto())
          )
        )
        return
    }

    // If we're currently leading this instance and the ballot we just received
    // is larger than the ballot we're using, then we should stop leading the
    // instance and yield to the replica with the higher ballot.
    if (leaderStates.contains(accept.instance) &&
        accept.ballot > leaderBallot(leaderStates(accept.instance))) {
      stopTimers(accept.instance)
      leaderStates -= accept.instance
    }

    // Update largestBallot.
    largestBallot = BallotHelpers.max(largestBallot, accept.ballot)

    // Reset recovery timer.
    recoverInstanceTimers.get(accept.instance).foreach(_.reset())

    // Update our command log.
    cmdLog -= accept.instance
    cmdLog(accept.instance) = AcceptedEntry(
      ballot = accept.ballot,
      voteBallot = accept.ballot,
      triple = CommandTriple(
        commandOrNoop = accept.commandOrNoop,
        sequenceNumber = accept.sequenceNumber,
        dependencies = InstancePrefixSet.fromProto(accept.dependencies)
      )
    )
    updateConflictIndex(accept.instance, accept.commandOrNoop)

    replica.send(
      ReplicaInbound().withAcceptOk(
        AcceptOk(instance = accept.instance,
                 ballot = accept.ballot,
                 replicaIndex = index)
      )
    )
  }

  private def handleAcceptOk(
      src: Transport#Address,
      acceptOk: AcceptOk
  ): Unit = {
    leaderStates.get(acceptOk.instance) match {
      case None =>
        logger.debug(
          s"Replica received an AcceptOk in instance ${acceptOk.instance} " +
            s"but is not leading the instance."
        )

      case Some(_: PreAccepting) =>
        logger.debug(
          s"Replica received an AcceptOk in instance ${acceptOk.instance} " +
            s"but is pre-accepting."
        )

      case Some(_: Preparing) =>
        logger.debug(
          s"Replica received an AcceptOk in instance ${acceptOk.instance} " +
            s"but is preparing."
        )

      case Some(
          accepting @ Replica.Accepting(ballot,
                                        triple,
                                        responses,
                                        resendAcceptsTimer)
          ) =>
        if (acceptOk.ballot != ballot) {
          logger.debug(
            s"Replica received an AcceptOk in ballot ${acceptOk.instance} " +
              s"but is currently leading ballot $ballot."
          )
          // If acceptOk.ballot were larger, then we would have received a
          // Nack instead of an AcceptOk.
          logger.checkLt(acceptOk.ballot, ballot)(BallotHelpers.Ordering)
          return
        }

        responses -= acceptOk.replicaIndex
        responses(acceptOk.replicaIndex) = acceptOk

        // We don't have enough responses yet.
        if (responses.size < config.slowQuorumSize) {
          return
        }

        // We have a quorum of replies. Commit the triple.
        commit(acceptOk.instance, triple, informOthers = true)
    }
  }

  private def handleCommit(src: Transport#Address, c: Commit): Unit = {
    commit(
      c.instance,
      CommandTriple(c.commandOrNoop,
                    c.sequenceNumber,
                    InstancePrefixSet.fromProto(c.dependencies)),
      informOthers = false
    )
  }

  private def handleNack(src: Transport#Address, nack: Nack): Unit = {
    largestBallot = BallotHelpers.max(largestBallot, nack.largestBallot)

    leaderStates.get(nack.instance) match {
      case None =>
        logger.debug(
          s"Replica received a nack of ballot ${nack.largestBallot} for " +
            s"instance ${nack.instance}, but is not currently leading the " +
            s"instance."
        )
        return

      case Some(preAccepting: PreAccepting) =>
        if (preAccepting.ballot >= nack.largestBallot) {
          logger.debug(
            s"Replica received a nack of ballot ${nack.largestBallot} for " +
              s"instance ${nack.instance}, but is currently preAccepting the " +
              s"instance in ballot ${preAccepting.ballot}."
          )
          return
        }

      case Some(accepting: Accepting) =>
        if (accepting.ballot >= nack.largestBallot) {
          logger.debug(
            s"Replica received a nack of ballot ${nack.largestBallot} for " +
              s"instance ${nack.instance}, but is currently accepting the " +
              s"instance in ballot ${accepting.ballot}."
          )
          return
        }

      case Some(preparing: Preparing) =>
        if (preparing.ballot >= nack.largestBallot) {
          logger.debug(
            s"Replica received a nack of ballot ${nack.largestBallot} for " +
              s"instance ${nack.instance}, but is currently preparing the " +
              s"instance in ballot ${preparing.ballot}."
          )
          return
        }
    }

    // If we get a Nack, it's possible there's another replica trying to
    // recover this instance. To avoid dueling replicas, we wait a bit to
    // recover.
    recoverInstanceTimers.get(nack.instance) match {
      case Some(timer) => timer.reset()
      case None =>
        recoverInstanceTimers(nack.instance) = makeRecoverInstanceTimer(
          nack.instance
        )
    }
  }

  private def handlePrepare(
      src: Transport#Address,
      prepare: Prepare
  ): Unit = {
    // Update largestBallot.
    largestBallot = BallotHelpers.max(largestBallot, prepare.ballot)

    // Reset recovery timer.
    recoverInstanceTimers.get(prepare.instance).foreach(_.reset())

    // If we're currently leading this instance and the ballot we just received
    // is larger than the ballot we're using, then we should stop leading the
    // instance and yield to the replica with the higher ballot.
    if (leaderStates.contains(prepare.instance) &&
        prepare.ballot > leaderBallot(leaderStates(prepare.instance))) {
      stopTimers(prepare.instance)
      leaderStates -= prepare.instance
    }

    val replica = chan[Replica[Transport]](src, Replica.serializer)
    val nack = ReplicaInbound().withNack(Nack(prepare.instance, largestBallot))
    cmdLog.get(prepare.instance) match {
      case None =>
        replica.send(
          ReplicaInbound().withPrepareOk(
            PrepareOk(
              ballot = prepare.ballot,
              instance = prepare.instance,
              replicaIndex = index,
              voteBallot = Replica.nullBallot,
              status = CommandStatus.NotSeen,
              commandOrNoop = None,
              sequenceNumber = None,
              dependencies = None
            )
          )
        )
        cmdLog(prepare.instance) = NoCommandEntry(prepare.ballot)

      case Some(NoCommandEntry(ballot)) =>
        // Don't process messages from old ballots. Note that we use `<`
        // instead of `<=` so that a replica can re-send a reply back to the
        // leader. If we used `<=`, then leaders would have to time out and
        // increment their ballots.
        if (prepare.ballot < ballot) {
          replica.send(nack)
          return
        }

        replica.send(
          ReplicaInbound().withPrepareOk(
            PrepareOk(
              ballot = prepare.ballot,
              instance = prepare.instance,
              replicaIndex = index,
              voteBallot = Replica.nullBallot,
              status = CommandStatus.NotSeen,
              commandOrNoop = None,
              sequenceNumber = None,
              dependencies = None
            )
          )
        )
        cmdLog -= prepare.instance
        cmdLog(prepare.instance) = NoCommandEntry(prepare.ballot)

      case Some(entry @ PreAcceptedEntry(ballot, voteBallot, triple)) =>
        // Don't process messages from old ballots.
        if (prepare.ballot < ballot) {
          replica.send(nack)
          return
        }

        replica.send(
          ReplicaInbound().withPrepareOk(
            PrepareOk(
              ballot = prepare.ballot,
              instance = prepare.instance,
              replicaIndex = index,
              voteBallot = voteBallot,
              status = CommandStatus.PreAccepted,
              commandOrNoop = Some(triple.commandOrNoop),
              sequenceNumber = Some(triple.sequenceNumber),
              dependencies = Some(triple.dependencies.toProto())
            )
          )
        )
        cmdLog -= prepare.instance
        cmdLog(prepare.instance) = entry.copy(ballot = prepare.ballot)

      case Some(entry @ AcceptedEntry(ballot, voteBallot, triple)) =>
        // Don't process messages from old ballots.
        if (prepare.ballot < ballot) {
          replica.send(nack)
          return
        }

        replica.send(
          ReplicaInbound().withPrepareOk(
            PrepareOk(
              ballot = prepare.ballot,
              instance = prepare.instance,
              replicaIndex = index,
              voteBallot = voteBallot,
              status = CommandStatus.Accepted,
              commandOrNoop = Some(triple.commandOrNoop),
              sequenceNumber = Some(triple.sequenceNumber),
              dependencies = Some(triple.dependencies.toProto())
            )
          )
        )
        cmdLog -= prepare.instance
        cmdLog(prepare.instance) = entry.copy(ballot = prepare.ballot)

      case Some(CommittedEntry(triple)) =>
        // The command has already been committed. No need to run the protocol.
        replica.send(
          ReplicaInbound().withCommit(
            Commit(instance = prepare.instance,
                   commandOrNoop = triple.commandOrNoop,
                   sequenceNumber = triple.sequenceNumber,
                   dependencies = triple.dependencies.toProto())
          )
        )
    }
  }

  private def handlePrepareOk(
      src: Transport#Address,
      prepareOk: PrepareOk
  ): Unit = {
    leaderStates.get(prepareOk.instance) match {
      case None =>
        logger.debug(
          s"Replica received a PrepareOk in instance ${prepareOk.instance} " +
            s"but is not leading the instance."
        )

      case Some(_: PreAccepting) =>
        logger.debug(
          s"Replica received a PrepareOk in instance ${prepareOk.instance} " +
            s"but is pre-accepting."
        )

      case Some(_: Accepting) =>
        logger.debug(
          s"Replica received a PrepareOk in instance ${prepareOk.instance} " +
            s"but is accepting."
        )

      case Some(
          preparing @ Replica.Preparing(ballot, responses, resendAcceptsTimer)
          ) =>
        if (prepareOk.ballot != ballot) {
          logger.debug(
            s"Replica received a preAcceptOk in ballot ${prepareOk.instance} " +
              s"but is currently leading ballot $ballot."
          )
          // If prepareOk.ballot were larger, then we would have received a
          // Nack instead of a PrepareOk.
          logger.checkLt(prepareOk.ballot, ballot)(BallotHelpers.Ordering)
          return
        }

        responses(prepareOk.replicaIndex) = prepareOk

        // If we don't have a quorum of responses yet, we have to wait to get
        // one.
        if (responses.size < config.slowQuorumSize) {
          return
        }

        // Look only at the ballots from the highest ballot, just like in
        // Paxos.
        val maxBallot =
          responses.values.map(_.voteBallot).max(BallotHelpers.Ordering)
        val prepareOks = responses.values.filter(_.voteBallot == maxBallot)

        // If some response was accepted, then we go with it. We are guaranteed
        // that there is only one such response. This is like getting a
        // response in a classic round during Fast Paxos.
        prepareOks.find(_.status == Some(CommandStatus.Accepted)) match {
          case Some(accepted) =>
            transitionToAcceptPhase(
              prepareOk.instance,
              ballot,
              CommandTriple(
                accepted.commandOrNoop.get,
                accepted.sequenceNumber.get,
                InstancePrefixSet.fromProto(accepted.dependencies.get)
              )
            )
            return

          case None =>
        }

        // If we have f matching responses in the fast round, excluding the
        // leader, then we go with it. Clearly, there can only be one response
        // with f matches out of f+1 responses. This is like checking if any
        // response has a majority of the quorum in Fast Paxos.
        val preAcceptsInDefaultBallotNotFromLeader: Seq[CommandTriple] =
          prepareOks
            .filter(_.status == CommandStatus.PreAccepted)
            .filter(p => p.ballot == Ballot(0, p.instance.replicaIndex))
            .filter(_.replicaIndex != index)
            .to[Seq]
            .map(
              p =>
                CommandTriple(p.commandOrNoop.get,
                              p.sequenceNumber.get,
                              InstancePrefixSet.fromProto(p.dependencies.get))
            )
        val candidates =
          Util.popularItems(preAcceptsInDefaultBallotNotFromLeader, config.f)
        if (candidates.size > 0) {
          logger.checkEq(candidates.size, 1)
          transitionToAcceptPhase(prepareOk.instance, ballot, candidates.head)
          return
        }

        // If no command had f matching responses, then we nothing was chosen
        // on the fast path. Now, we just start the protocol over with a
        // command (if there is one) or with a noop if there isn't.
        prepareOks.find(_.status == CommandStatus.PreAccepted) match {
          case Some(preAccepted) =>
            transitionToPreAcceptPhase(prepareOk.instance,
                                       ballot,
                                       preAccepted.commandOrNoop.get,
                                       avoidFastPath = true)
          case None =>
            transitionToPreAcceptPhase(prepareOk.instance,
                                       ballot,
                                       CommandOrNoop().withNoop(Noop()),
                                       avoidFastPath = true)
        }
    }
  }
}
