package frankenpaxos.epaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import scala.collection.mutable
import scala.scalajs.js.annotation._

// By default, Scala case classes cannot be compared using comparators like `<`
// or `>`. This is particularly annoying for Ballots because we often want to
// compare them. The BallotsOrdering implicit class below allows us to do just
// that.
@JSExportAll
object BallotHelpers {
  private val ordering = scala.math.Ordering.Tuple2[Int, Int]

  private def ballotToTuple(ballot: Ballot): (Int, Int) = {
    val Ballot(ordering, replicaIndex) = ballot
    (ordering, replicaIndex)
  }

  private def tupleToBallot(t: (Int, Int)): Ballot = {
    val (ordering, replicaIndex) = t
    Ballot(ordering, replicaIndex)
  }

  def max(lhs: Ballot, rhs: Ballot): Ballot = {
    tupleToBallot(ordering.max(ballotToTuple(lhs), ballotToTuple(rhs)))
  }

  implicit class BallotOrdering(lhs: Ballot) {
    def <(rhs: Ballot) = ordering.lt(ballotToTuple(lhs), ballotToTuple(rhs))
    def <=(rhs: Ballot) = ordering.lteq(ballotToTuple(lhs), ballotToTuple(rhs))
    def >(rhs: Ballot) = ordering.gt(ballotToTuple(lhs), ballotToTuple(rhs))
    def >=(rhs: Ballot) = ordering.gteq(ballotToTuple(lhs), ballotToTuple(rhs))
  }
}

import BallotHelpers.BallotOrdering

@JSExportAll
object ReplicaInboundSerializer extends ProtoSerializer[ReplicaInbound] {
  type A = ReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer
}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    // Public for Javascript.
    val stateMachine: StateMachine
) extends Actor(address, transport, logger) {
  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer
  type ReplicaIndex = Int

  // Fields ////////////////////////////////////////////////////////////////////
  logger.check(config.valid())
  logger.check(config.replicaAddresses.contains(address))
  private val index: ReplicaIndex = config.replicaAddresses.indexOf(address)

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (replicaAddress <- config.replicaAddresses)
      yield chan[Replica[Transport]](replicaAddress, Replica.serializer)

  private val otherReplicas: Seq[Chan[Replica[Transport]]] =
    for (a <- config.replicaAddresses if a != address)
      yield chan[Replica[Transport]](a, Replica.serializer)

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
  //
  // Every replica maintains a local instance number i, initially 0. When a
  // replica R receives a command, it assigns the command instance R.i and then
  // increments i. Thus, every replica assigns cmdLog vertically within its
  // column of the array from bottom to top.
  //
  // `CmdLogEntry` represents the data within a cell. `cmdLog` represents
  // the array (though we implement it as a map). `nextAvailableInstance`
  // represents i. Note that replicas are not represented with names like Q, R,
  // S, but with integer-valued indexes.
  @JSExportAll
  case class CmdLogEntry(
      // A command and its sequence number, dependencies, and status. See the
      // EPaxos paper for a description of what these are.
      command: Command,
      sequenceNumber: Int,
      dependencies: mutable.Set[Instance],
      status: CommandStatus,
      // The EPaxos paper and technical report are a tiny bit vague as to how
      // and when ballots should be updated. After skimming through EPaxos'
      // TLA+ specification and implementation, we believe that EPaxos might
      // have a small bug in how it implements ballots. The TLA+ spec and
      // implementation have a single ballot for every instance, but we think
      // that we need two (like with Paxos) in order to maintain correctness.
      //
      // Thus, we have ballot and voteBallot that play the same role as a Paxos
      // acceptor. ballot is the largest ballot this replica has seen for this
      // instance. voteBallot is the largest ballot in which the replica has
      // voted for a command (i.e. processed a PreAccept or Accept). Note that
      // we haven't proven this is correct, so it may also be wrong.
      ballot: Ballot,
      voteBallot: Ballot
  )

  @JSExport
  protected val cmdLog: mutable.Map[Instance, CmdLogEntry] =
    mutable.Map[Instance, CmdLogEntry]()

  @JSExport
  protected var nextAvailableInstance: Int = 0

  // The lowest possible ballot used by this replica.
  @JSExport
  protected val lowestBallot: Ballot = Ballot(0, index)

  // The largest ballot ever seen by this replica. largestBallot is used when a
  // replica receives a nack and needs to choose a larger ballot.
  @JSExport
  protected var largestBallot: Ballot = Ballot(0, index)

  // TODO(mwhittaker): Add a client table. Potentially pull out some code from
  // Fast MultiPaxos so that client table code is shared across protocols.

  // DELETE(mwhittaker):
  // Replica responses.
  // var preacceptResponses =
  //   mutable.Map[Instance, mutable.Map[ReplicaIndex, PreAcceptOk]]()
  // var acceptResponses =
  //   mutable.Map[Instance, mutable.Map[ReplicaIndex, AcceptOk]]()
  // var prepareResponses =
  //   mutable.Map[Instance, mutable.Map[ReplicaIndex, PrepareOk]]()

  // When a replica receives a command from a client, it becomes the leader of
  // the command, the designated replica that is responsible for driving the
  // protocol through its phases to get the command chosen. LeaderState
  // represents the state of a leader during various points in the lifecycle of
  // the protocol, whether the leader is pre-accepting, accepting, or preparing
  // (during recovery).
  sealed trait LeaderState

  case class PreAccepting(
      // Every EPaxos replica plays the role of a Paxos proposer _and_ a Paxos
      // acceptor. The ballot and voteBallot in a command log entry are used
      // when the replica acts like an acceptor. leaderBallots is used when a
      // replica acts like a leader. In particular, leaderBallots[instance] is
      // the ballot in which the replica is trying to get a value chosen. This
      // value is like the ballot stored by a Paxos proposer.
      //
      // Note that this implementation of ballots differs from the one in
      // EPaxos' TLA+ spec, but we think this is more likely to be correct.
      ballot: Ballot,
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
  ) extends LeaderState

  case class Accepting(
      // See above.
      ballot: Ballot,
      // AcceptOk responses, indexed by the replica that sent them.
      responses: mutable.Map[ReplicaIndex, AcceptOk],
      // A timer to re-send Accepts.
      resendAcceptsTimer: Transport#Timer
  ) extends LeaderState

  case class Preparing(
      // See above.
      ballot: Ballot,
      // Prepare responses, indexed by the replica that sent them.
      responses: mutable.Map[ReplicaIndex, AcceptOk],
      // A timer to re-send Prepares.
      resendPreparesTimer: Transport#Timer
  ) extends LeaderState

  @JSExport
  protected val leaderStates = mutable.Map[Instance, LeaderState]()

  // TODO(mwhittaker): Add dependency graph.

  // TODO(mwhittaker): Use generic state machine.
  // val stateMachine: KeyValueStore = new KeyValueStore()
  // TODO(mwhittaker): Remove.
  // private val executedCommands: mutable.Set[Command] = mutable.Set()
  // TODO(mwhittaker): Remove.
  // var conflictsMap =
  //   mutable.Map[String, (mutable.Set[Instance], mutable.Set[Instance])]()
  // TODO(mwhittaker): Remove.
  // val removeCommands: mutable.Set[Instance] = mutable.Set[Instance]()

  // var graph: DependencyGraph = new DependencyGraph()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def preAcceptingSlowPath(preAccepting: PreAccepting): Unit = {
    // TODO(mwhittaker): Implement.
    // TODO(mwhittaker): Don't forget to stop the timers!
    ???
  }

  // Timers ////////////////////////////////////////////////////////////////////
  private def makeResendPreAcceptsTimer(
      preAccept: PreAccept
  ): Transport#Timer = {
    // TODO(mwhittaker): Pull this duration out into an option.
    lazy val t: Transport#Timer = timer(
      s"resendPreAccepts ${preAccept.instance} ${preAccept.ballot}",
      java.time.Duration.ofMillis(500),
      () => {
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
    // TODO(mwhittaker): Pull this duration out into an option.
    val t = timer(
      s"defaultToSlowPath ${instance}",
      java.time.Duration.ofMillis(500),
      () => {
        leaderStates.get(instance) match {
          case None | Some(Accepting(_, _, _)) | Some(Preparing(_, _, _)) =>
            logger.fatal(
              "defaultToSlowPath timer triggered but replica is not " +
                "preAccepting."
            )
          case Some(preAccepting @ PreAccepting(_, _, _, _, _)) =>
            preAcceptingSlowPath(preAccepting)
        }
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
    inbound.request match {
      case Request.ClientRequest(r) => handleClientRequest(src, r)
      case Request.PreAccept(r)     => handlePreAccept(src, r)
      case Request.PreAcceptOk(r)   => handlePreAcceptOk(src, r)
      case Request.Accept(r)        => handleAccept(src, r)
      case Request.AcceptOk(r)      => handleAcceptOk(src, r)
      case Request.Commit(r)        => handleCommit(src, r)
      case Request.Prepare(r)       => handlePrepare(src, r)
      case Request.PrepareOk(r)     => handlePrepareOk(src, r)
      case Request.Nack(r)          => handleNack(src, r)
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  // TODO(mwhittaker): This method has to add 1 to the maximum sequence number.
  //  private def findAttributes(
  //      command: Command,
  //      commandInstance: Instance
  //  ): (mutable.Set[Instance], Int) = {
  //    var deps: mutable.Set[Instance] = mutable.Set()
  //    var maxSeqNum: Int = 0
  //
  //    val commandString = command.command.toStringUtf8
  //    val tokens = commandString.split(" ")
  //
  //    val action = tokens(0)
  //    val key = tokens(1)
  //
  //    val tupleSet: (mutable.Set[Instance], mutable.Set[Instance]) =
  //      conflictsMap.getOrElse(key,
  //                             (mutable.Set[Instance](), mutable.Set[Instance]()))
  //
  //    action match {
  //      case "GET" => deps = deps.union(tupleSet._2)
  //      case "SET" => deps = deps.union(tupleSet._1).union(tupleSet._2)
  //    }
  //
  //    if (deps.isEmpty) {
  //      return (deps, 0)
  //    }
  //
  //    // TODO(mwhittaker): This doesn't compute the maximum sequence number, I
  //    // don't think.
  //    val state = cmdLog.get(deps.head)
  //    if (state.nonEmpty) {
  //      maxSeqNum = Math.max(maxSeqNum, state.get.sequenceNumber)
  //    }
  //    deps.remove(commandInstance)
  //    (deps, maxSeqNum)
  //  }
  //
  //  private def updateConflictMap(command: Command, instance: Instance): Unit = {
  //    val commandString = command.command.toStringUtf8
  //    val tokens = commandString.split(" ")
  //
  //    val action = tokens(0)
  //    val key = tokens(1)
  //
  //    val tupleSet: (mutable.Set[Instance], mutable.Set[Instance]) =
  //      conflictsMap.getOrElse(key,
  //                             (mutable.Set[Instance](), mutable.Set[Instance]()))
  //
  //    action match {
  //      case "GET" => tupleSet._1.add(instance)
  //      case "SET" => tupleSet._2.add(instance)
  //    }
  //
  //    conflictsMap.put(key, tupleSet)
  //    logger.info("Tuple set: " + tupleSet.toString())
  //  }

  private def handleClientRequest(
      src: Transport#Address,
      request: ClientRequest
  ): Unit = {
    // TODO(mwhittaker): Check the client table. If the command already exists
    // in the client table, then we can return it immediately.

    val instance: Instance = Instance(index, nextAvailableInstance)
    nextAvailableInstance += 1

    // Update the command log.
    // TODO(mwhittaker): Compute sequence number and dependencies.
    cmdLog.put(
      instance,
      CmdLogEntry(
        command = request.command,
        sequenceNumber = ???, // TODO(mwhittaker): Implement.
        dependencies = ???, // TODO(mwhittaker): Implement.
        status = CommandStatus.PreAccepted,
        ballot = lowestBallot,
        voteBallot = lowestBallot
      )
    )

    // Send PreAccept messages to all other replicas.
    //
    // TODO(mwhittaker): Maybe add thriftiness. Thriftiness is less important
    // for basic EPaxos since the fast quorum sizes are so big.
    val preAccept = PreAccept(
      command = request.command,
      sequenceNumber = ???, // TODO(mwhittaker): Implement.
      dependencies = ???, // TODO(mwhittaker): Implement.  commandInstance =
      instance,
      ballot = lowestBallot
    )
    otherReplicas.foreach(_.send(ReplicaInbound().withPreAccept(preAccept)))

// Update our leader state.
    logger.check(!leaderStates.contains(instance))
    leaderStates(instance) = PreAccepting(
      ballot = lowestBallot,
      responses = mutable.Map[ReplicaIndex, PreAcceptOk](
        index -> PreAcceptOk(
          command = request.command,
          sequenceNumber = ???, // TODO(mwhittaker): Implement.
          dependencies = ???, // TODO(mwhittaker): Implement.
          instance = instance,
          ballot = lowestBallot,
          replicaIndex = index
        )
      ),
      avoidFastPath = false,
      resendPreAcceptsTimer = makeResendPreAcceptsTimer(preAccept),
      defaultToSlowPathTimer = None
    )
  }

  private def handlePreAccept(
      src: Transport#Address,
      preAccept: PreAccept
  ): Unit = {
    // Nack the PreAccept if it's from an older ballot.
    cmdLog.get(preAccept.instance) match {
      case Some(entry) =>
        if (preAccept.ballot < entry.ballot) {
          logger.warn(
            s"Replica $index received a PreAccept in ballot " +
              s"${preAccept.ballot} of instance ${preAccept.instance} but " +
              s"already processed a message in ballot ${entry.ballot}."
          )
          val replica = chan[Replica[Transport]](src, Replica.serializer)
          replica.send(
            ReplicaInbound().withNack(Nack(preAccept.instance, largestBallot))
          )
          return
        }

      case None =>
      // We haven't seen anything for this instance before, so we're good.
    }

    // Ignore the PreAccept if we've already voted.
    cmdLog.get(preAccept.instance) match {
      case Some(entry) =>
        if (preAccept.ballot <= entry.voteBallot) {
          logger.warn(
            s"Replica $index received a PreAccept in ballot " +
              s"${preAccept.ballot} of instance ${preAccept.instance} but " +
              s"already voted in ballot ${entry.ballot}."
          )
          return
        }

      case None =>
      // We haven't seen anything for this instance before, so we're good.
    }

    // Update largestBallot.
    largestBallot = BallotHelpers.max(largestBallot, preAccept.ballot)

    // TODO(mwhittaker): Compute dependencies and sequence number, taking into
    // consideration the ones in the PreAccept.

    cmdLog.put(
      preAccept.instance,
      CmdLogEntry(
        command = preAccept.command,
        sequenceNumber = ???, // TODO(mwhittaker): Implement.
        dependencies = ???, // TODO(mwhittaker): Implement.
        status = CommandStatus.PreAccepted,
        ballot = preAccept.ballot,
        voteBallot = preAccept.ballot
      )
    )

    val leader = chan[Replica[Transport]](src, Replica.serializer)
    leader.send(
      ReplicaInbound().withPreAcceptOk(
        PreAcceptOk(
          command = preAccept.command,
          sequenceNumber = ???, // TODO(mwhittaker): Implement.
          dependencies = ???, // TODO(mwhittaker): Implement.
          instance = preAccept.instance,
          ballot = preAccept.ballot,
          replicaIndex = index
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
        logger.warn(
          s"Replica received a PreAcceptOk in instance " +
            s"${preAcceptOk.instance} but is not leading the instance."
        )

      case Some(Accepting(_, _, _)) =>
        logger.warn(
          s"Replica received a PreAcceptOk in instance " +
            s"${preAcceptOk.instance} but is accepting."
        )

      case Some(Preparing(_, _, _)) =>
        logger.warn(
          s"Replica received a PreAcceptOk in instance " +
            s"${preAcceptOk.instance} but is preparing."
        )

      case Some(
          preAccepting @ PreAccepting(ballot,
                                      responses,
                                      avoidFastPath,
                                      resendPreAcceptsTimer,
                                      defaultToSlowPathTimer)
          ) =>
        if (preAcceptOk.ballot != ballot) {
          logger.warn(
            s"Replica received a preAcceptOk in ballot " +
              s"${preAcceptOk.instance} but is currently leading ballot " +
              s"$ballot."
          )
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
          // Do nothing.
          return
        }

        // If we've achieved a classic quorum for the first time (and we're not
        // avoiding the fast path), we still want to wait for a fast quorum,
        // but we need to set a timer to default to taking the slow path.
        if (avoidFastPath &&
            oldNumberOfResponses < config.slowQuorumSize &&
            newNumberOfResponses >= config.slowQuorumSize) {
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
          preAcceptingSlowPath(preAccepting)
          return
        }

        // If we've received a fast quorum of responses, we can take the fast
        // path!
        if (newNumberOfResponses >= config.fastQuorumSize) {
          logger.check(!avoidFastPath)

          // We extract all (seq, deps) pairs in the PreAcceptOks, excluding our
          // own. If any appears n-2 times or more, we're good to take the fast
          // path.
          val seqDeps = responses
            .filterKeys(_ != index)
            .values
            .to[Seq]
            .map(p => (p.sequenceNumber, p.dependencies))
          val candidates = Util.popularItems(seqDeps, config.fastQuorumSize - 1)
          if (candidates.size > 0) {
            logger.check_eq(candidates.size, 1)
            // TODO(mwhittaker): Start the fast path with the candidate.
            // set status to committed
            // to some stuff to execute
            // send out commit messages
            // TODO(mwhittaker): Don't forget to stop the timers.
          } else {
            // There were not enough matching (seq, deps) pairs. We have to
            // resort to the slow path.
            preAcceptingSlowPath(preAccepting)
          }
        }
    }

    // add to responses
    // if exactly a classic quorum (and not avoid):
    //   start timer for defaulting to classic
    // else if fast quroum:
    //   stop defaulting to classic timer
    //   check to see if we have a quorum of matching responses
    //   if so:
    //     accept and send accepts
    //   else:
    //     compute deps and stuff and start next phase

    // TODO(mwhittaker): During recovery, isn't it possible that a replica is
    // received PreAcceptOks for an instance it doesn't own? Figure out if this
    // code is correct.
    //
    // // Verify leader is receiving responses
    // if (preAcceptOk.commandInstance.replicaIndex != index) {
    //   return
    // }

    // TODO(mwhittaker): Handle fast path. Right now, I think this always goes
    // to the slow path.
    val preAcceptOkSet: mutable.Buffer[PreAcceptOk] = preacceptResponses
      .getOrElse(preAcceptOk.commandInstance, mutable.Buffer.empty)

    preAcceptOkSet.append(preAcceptOk)
    preacceptResponses.put(preAcceptOk.commandInstance, preAcceptOkSet)

    if (preacceptResponses
          .getOrElse(preAcceptOk.commandInstance, mutable.Buffer.empty)
          .size < config.slowQuorumSize) {
      // TODO(mwhittaker): Add a debug log maybe?
      return
    }

    val N: Int = config.replicaAddresses.size
    val seqTuple: (Boolean, Option[Int]) = checkSameSequenceNumbers(
      preacceptResponses.getOrElse(preAcceptOk.commandInstance,
                                   mutable.Buffer.empty),
      N - 1
    )
    val depsTuple: (Boolean, Option[Seq[Instance]]) = checkSameDependencies(
      preacceptResponses.getOrElse(preAcceptOk.commandInstance,
                                   mutable.Buffer.empty),
      N - 1
    )

    if (seqTuple._1 && depsTuple._1 && !preAcceptOk.avoid) {
      // TODO(mwhittaker): Send response to client.

      // Run commit phase
      runCommitPhase(preAcceptOk.command,
                     seqTuple._2.get,
                     depsTuple._2.get,
                     preAcceptOk.commandInstance)
    } else {
      var updatedDeps: mutable.Set[Instance] = mutable.Set()
      var maxSequenceNumber: Int = preAcceptOk.sequenceNumber
      for (preacceptResponse <- preacceptResponses.getOrElse(
             preAcceptOk.commandInstance,
             mutable.Buffer.empty
           )) {
        updatedDeps =
          updatedDeps.union(mutable.Set(preacceptResponse.dependencies: _*))
        maxSequenceNumber =
          Math.max(maxSequenceNumber, preacceptResponse.sequenceNumber)
      }
      startPaxosAcceptPhase(preAcceptOk.command,
                            maxSequenceNumber,
                            updatedDeps,
                            preAcceptOk.commandInstance)
    }
  }

  private def startPhaseOne(
      instance: Instance,
      newCommand: Command,
      value: PrepareOk
  ): Unit = {
    val attributes = findAttributes(newCommand, value.instance)
    val seqCommand: Int = attributes._2
    val seqDeps: mutable.Set[Instance] = attributes._1
    cmdLog.put(
      instance,
      CmdLogEntry(newCommand,
                  seqCommand,
                  seqDeps,
                  CommandStatus.PreAccepted,
                  ???)
    )
    // ballotMapping.put(instance, value.ballot)

    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != config.fastQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(
          ReplicaInbound().withPreAccept(
            PreAccept(
              command = newCommand,
              sequenceNumber = seqCommand,
              dependencies = seqDeps.toSeq,
              commandInstance = instance,
              avoid = true,
              ballot = ??? ///ballotMapping.getOrElse(instance, value.ballot)
            )
          )
        )
        count += 1
      }
      setIndex += 1
    }

    val buffer =
      preacceptResponses.getOrElse(instance, mutable.Buffer[PreAcceptOk]())
    val message = PreAcceptOk(
      command = value.command,
      sequenceNumber = seqCommand,
      dependencies = seqDeps.toSeq,
      commandInstance = instance,
      avoid = true,
      ballot = ???, // ballotMapping.getOrElse(instance, largestBallot),
      replicaIndex = index
    )
    buffer.append(message)
    preacceptResponses.put(instance, buffer)
  }

  private def checkSameSequenceNumbers(
      preAcceptResponses: mutable.Buffer[PreAcceptOk],
      numSameNeeded: Int
  ): (Boolean, Option[Int]) = {
    val sequenceNumCount: mutable.Map[Int, Int] = mutable.Map()
    for (preacceptOk <- preAcceptResponses) {
      sequenceNumCount.put(
        preacceptOk.sequenceNumber,
        sequenceNumCount.getOrElse(preacceptOk.sequenceNumber, 0) + 1
      )
    }

    for (key <- sequenceNumCount.keys) {
      if (sequenceNumCount.getOrElse(key, 0) >= numSameNeeded) {
        return (true, Some(key))
      }
    }

    (false, None)
  }

  private def checkSameDependencies(
      preAcceptResponses: mutable.Buffer[PreAcceptOk],
      numSameNeeded: Int
  ): (Boolean, Option[Seq[Instance]]) = {
    val dependenciesNumCount: mutable.Map[Seq[Instance], Int] = mutable.Map()
    for (preacceptOk <- preAcceptResponses) {
      dependenciesNumCount.put(
        preacceptOk.dependencies,
        dependenciesNumCount.getOrElse(preacceptOk.dependencies, 0) + 1
      )
    }

    for (key <- dependenciesNumCount.keys) {
      if (dependenciesNumCount.getOrElse(key, 0) >= numSameNeeded) {
        return (true, Some(key))
      }
    }

    (false, None)
  }

  private def startPaxosAcceptPhase(
      command: Command,
      sequenceNum: Int,
      deps: mutable.Set[Instance],
      commandInstance: Instance
  ): Unit = {
    cmdLog.put(
      commandInstance,
      CmdLogEntry(command, sequenceNum, deps, CommandStatus.Accepted, ???)
    )
    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != config.slowQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(
          ReplicaInbound().withAccept(
            Accept(
              command,
              sequenceNum,
              deps.toSeq,
              commandInstance,
              ??? // ballotMapping.getOrElse(commandInstance, lowestBallot)
            )
          )
        )
        count += 1
      }
      setIndex += 1
    }

    // Add leader accept ok response
    val buffer =
      acceptResponses.getOrElse(commandInstance, mutable.Buffer[AcceptOk]())
    val message = AcceptOk(
      command = command,
      commandInstance = commandInstance,
      ballot = ???, //ballotMapping.getOrElse(commandInstance, lowestBallot),
      replicaIndex = index
    )
    buffer.append(message)
    acceptResponses.put(commandInstance, buffer)
  }

  private def handleAccept(src: Transport#Address, value: Accept): Unit = {
    if (value.ballot.ordering <
          42
        // ballotMapping
        //     .getOrElse(value.commandInstance, lowestBallot)
        //     .ordering
        ) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          Nack(
            oldBallot = ???,
            // ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }

    cmdLog.put(value.commandInstance,
               CmdLogEntry(value.command,
                           value.sequenceNumber,
                           mutable.Set(value.dependencies: _*),
                           CommandStatus.Accepted,
                           ???))
    // ballotMapping.put(value.commandInstance, value.ballot)
    val leader = replicas(value.commandInstance.replicaIndex)
    leader.send(
      ReplicaInbound().withAcceptOk(
        AcceptOk(
          command = value.command,
          commandInstance = value.commandInstance,
          ballot = ???, // ballotMapping.getOrElse(value.commandInstance, value.ballot),
          replicaIndex = index
        )
      )
    )
  }

  private def handleAcceptOk(
      src: Transport#Address,
      value: AcceptOk
  ): Unit = {
    // TODO(mwhittaker): Double check that NACKing here is appropriate.
    if (value.ballot.ordering < 42
        // ballotMapping
        // .getOrElse(value.commandInstance, lowestBallot)
        // .ordering
        ) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          Nack(
            oldBallot = ???,
            // ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }

    val responseSet: mutable.Buffer[AcceptOk] = acceptResponses.getOrElse(
      value.commandInstance,
      mutable.Buffer.empty
    )
    responseSet.append(value)
    acceptResponses.put(value.commandInstance, responseSet)
    // ballotMapping.put(value.commandInstance, value.ballot)

    if (responseSet.size >= config.slowQuorumSize) {
      // TODO(mwhittaker): Send reply to client.

      // run commit phase
      val commandState: Option[CmdLogEntry] =
        cmdLog.get(value.commandInstance)
      if (commandState.nonEmpty) {
        runCommitPhase(commandState.get.command,
                       commandState.get.sequenceNumber,
                       commandState.get.dependencies.toSeq,
                       value.commandInstance)
      }
    }
  }

  // TODO(mwhittaker): Is this correct? Can we remove cmdLog this eagerly?
  private def removeInstance(command: Command, instance: Instance): Unit = {
    val commandString = command.command.toStringUtf8
    val tokens = commandString.split(" ")

    val action = tokens(0)
    val key = tokens(1)

    val tupleSet: (mutable.Set[Instance], mutable.Set[Instance]) =
      conflictsMap.getOrElse(key,
                             (mutable.Set[Instance](), mutable.Set[Instance]()))

    action match {
      case "GET" => tupleSet._1.remove(instance)
      case "SET" => tupleSet._2.remove(instance)
    }

    conflictsMap.put(key, tupleSet)
  }

  private def runCommitPhase(
      command: Command,
      seqNum: Int,
      deps: Seq[Instance],
      instance: Instance
  ): Unit = {
    cmdLog.put(
      instance,
      CmdLogEntry(command,
                  seqNum,
                  mutable.Set(deps: _*),
                  CommandStatus.Committed,
                  ???)
    )

    // TODO(mwhittaker): I think this is adding committed cmdLog before the
    // command's dependencies are necessarily committed.
    recursiveAdd(instance)
    graph.executeDependencyGraph(stateMachine, executedCommands)

    for (inst <- removeCommands) {
      val st = cmdLog.get(inst)
      if (st.nonEmpty) {
        removeInstance(st.get.command, inst)
      }
      cmdLog.remove(inst)
    }
    removeCommands.clear()

    for (replica <- replicas) {
      replica.send(
        ReplicaInbound().withCommit(
          Commit(
            command,
            seqNum,
            deps,
            instance,
            ??? //ballotMapping.getOrElse(instance, lowestBallot)
          )
        )
      )
    }

  }

  private def recursiveAdd(instance: Instance): Unit = {
    val instanceQueue = mutable.Queue[Instance]()
    instanceQueue.enqueue(instance)

    while (instanceQueue.nonEmpty) {
      val headInstance = instanceQueue.dequeue()
      val headState = cmdLog.get(headInstance)

      if (headState.nonEmpty && headState.get.status == CommandStatus.Committed) {
        graph.directedGraph.addVertex(
          (headState.get.command, headState.get.sequenceNumber)
        )
        removeCommands.add(headInstance)

        for (dep <- headState.get.dependencies) {
          val depState = cmdLog.get(dep)
          if (depState.nonEmpty && depState.get.status == CommandStatus.Committed) {
            removeCommands.add(dep)
            graph.directedGraph.addVertex(
              (depState.get.command, depState.get.sequenceNumber)
            )

            if (!(headState.get.command.equals(depState.get.command) && headState.get.sequenceNumber
                  .equals(depState.get.sequenceNumber))) {
              graph.directedGraph.addEdge(
                (headState.get.command, headState.get.sequenceNumber),
                (depState.get.command, depState.get.sequenceNumber)
              )
            }

            instanceQueue.enqueue(dep)
          }
        }
      }
    }
  }

  private def handleCommit(src: Transport#Address, value: Commit): Unit = {
    // TODO(mwhittaker): You can't Nack a commit can you?
    if (value.ballot.ordering <
          42
        // ballotMapping
        //     .getOrElse(value.commandInstance, lowestBallot)
        //     .ordering
        ) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          Nack(
            oldBallot = ???,
            // ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }

    // TODO(mwhittaker): Call runCommitPhase here instead?

    cmdLog.put(value.commandInstance,
               CmdLogEntry(value.command,
                           value.sequenceNumber,
                           mutable.Set(value.dependencies: _*),
                           CommandStatus.Committed,
                           ???))

    recursiveAdd(value.commandInstance)
    graph.executeDependencyGraph(stateMachine, executedCommands)

    for (inst <- removeCommands) {
      val st = cmdLog.get(inst)
      if (st.nonEmpty) {
        removeInstance(st.get.command, inst)
      }
      cmdLog.remove(inst)
    }
    removeCommands.clear()
  }

  private def explicitPrepare(instance: Instance, oldBallot: Ballot): Unit = {
    val newBallot: Ballot = Ballot(oldBallot.ordering + 1, index)
    largestBallot = newBallot
    // ballotMapping.put(instance, largestBallot)
    for (replica <- replicas) {
      replica.send(
        ReplicaInbound().withPrepare(
          Prepare(
            ballot = newBallot,
            instance
          )
        )
      )
    }
  }

  private def handleNack(src: Transport#Address, value: Nack): Unit = {
    if (index == value.instance.replicaIndex) {
      explicitPrepare(value.instance, value.oldBallot)
    }
  }

  private def handlePrepare(
      src: Transport#Address,
      value: Prepare
  ): Unit = {
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    if (value.ballot.ordering > 42
        // ballotMapping
        //     .getOrElse(value.instance, lowestBallot)
        //     .ordering
        ) {
      // ballotMapping.put(value.instance, value.ballot)
      val state: Option[CmdLogEntry] = cmdLog.get(value.instance)
      if (state.nonEmpty) {
        replica.send(
          ReplicaInbound().withPrepareOk(
            PrepareOk(
              ballot = value.ballot,
              instance = value.instance,
              command = state.get.command,
              sequenceNumber = state.get.sequenceNumber,
              dependencies = state.get.dependencies.toSeq,
              status = state.get.status,
              replicaIndex = index
            )
          )
        )
      }
      // TODO(mwhittaker): What if the state is non-empty?
    } else {
      replica.send(
        ReplicaInbound().withNack(
          Nack(
            oldBallot = ???, //ballotMapping.getOrElse(value.instance, lowestBallot),
            instance = value.instance
          )
        )
      )
    }
  }

  private def checkPrepareOkSame(
      neededSize: Int,
      replicaIndex: Int,
      responses: mutable.Buffer[PrepareOk]
  ): (Boolean, PrepareOk, Boolean, PrepareOk) = {
    val responseMap: mutable.Map[PrepareOk, Int] = mutable.Map()
    val responseMapExclude: mutable.Map[PrepareOk, Int] = mutable.Map()

    for (response <- responses) {
      responseMap.put(response, responseMap.getOrElse(response, 0) + 1)
      if (response.instance.replicaIndex != replicaIndex) {
        responseMapExclude.put(response,
                               responseMapExclude.getOrElse(response, 0) + 1)
      }
    }

    for (response <- responseMapExclude.keys) {
      if (responseMapExclude.getOrElse(response, 0) >= neededSize) {
        return (true, response, true, response)
      }
    }

    for (response <- responseMap.keys) {
      if (responseMap.getOrElse(response, 0) >= 1) {
        return (false, null, true, response)
      }
    }

    (false, null, false, null)
  }

  private def handlePrepareOk(
      src: Transport#Address,
      value: PrepareOk
  ): Unit = {
    val prepareValues: mutable.Buffer[PrepareOk] =
      prepareResponses.getOrElse(value.instance, mutable.Buffer())
    prepareValues.append(value)
    if (prepareValues.size >= config.slowQuorumSize) {
      val maxBallot: Int =
        prepareValues.maxBy(_.ballot.ordering).ballot.ordering
      val R: mutable.Buffer[PrepareOk] =
        prepareValues.filter(_.ballot.ordering == maxBallot)
      val sameMessage: (Boolean, PrepareOk, Boolean, PrepareOk) =
        checkPrepareOkSame(config.n / 2, value.instance.replicaIndex, R)
      if (R.exists(_.status.equals(CommandStatus.Committed))) {
        val message: PrepareOk =
          R.filter(_.status.eq(CommandStatus.Committed)).head
        runCommitPhase(message.command,
                       message.sequenceNumber,
                       message.dependencies,
                       message.instance)
      } else if (R.exists(_.status.equals(CommandStatus.Accepted))) {
        val message: PrepareOk =
          R.filter(_.status.equals(CommandStatus.Accepted)).head
        startPaxosAcceptPhase(message.command,
                              message.sequenceNumber,
                              mutable.Set(message.dependencies: _*),
                              message.instance)
      } else if (sameMessage._1) {
        startPaxosAcceptPhase(sameMessage._2.command,
                              sameMessage._2.sequenceNumber,
                              mutable.Set(sameMessage._2.dependencies: _*),
                              sameMessage._2.instance)
      } else if (sameMessage._3) {
        startPhaseOne(sameMessage._4.instance,
                      sameMessage._4.command,
                      sameMessage._4)
      } else {
        startPhaseOne(sameMessage._4.instance,
                      Command(null, -1, ByteString.copyFromUtf8("Noop")),
                      sameMessage._4)
      }
    }
  }
}
