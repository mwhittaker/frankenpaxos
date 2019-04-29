package frankenpaxos.epaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.Util
import frankenpaxos.statemachine.KeyValueStore
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
object Replica {
  val serializer = ReplicaInboundSerializer
}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer

  logger.check(config.valid())
  logger.check(config.replicaAddresses.contains(address))
  private val index: Int = config.replicaAddresses.indexOf(address)

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (replicaAddress <- config.replicaAddresses)
      yield chan[Replica[Transport]](replicaAddress, Replica.serializer)

  var nextAvailableInstance: Int = 1

  sealed trait ReplicaStatus
  object PreAccepted extends ReplicaStatus
  object Accepted extends ReplicaStatus
  object Committed extends ReplicaStatus
  object Executed extends ReplicaStatus

  case class State(
      command: Command,
      sequenceNumber: Int,
      dependencies: mutable.Set[Instance],
      status: ReplicaStatus
  )

  var preacceptResponses: mutable.Map[Instance, mutable.Buffer[PreAcceptOk]] =
    mutable.Map()

  var acceptOkResponses: mutable.Map[Instance, mutable.Buffer[AcceptOk]] =
    mutable.Map()

  var prepareResponses: mutable.Map[Instance, mutable.Buffer[PrepareOk]] =
    mutable.Map()

  private val instanceClientMapping: mutable.Map[Instance, Transport#Address] =
    mutable.Map()

  // TODO(mwhittaker): Use generic state machine.
  val stateMachine: KeyValueStore = new KeyValueStore()

  private val lowestBallot: Ballot = Ballot(0, index)
  private var currentBallot: Ballot = Ballot(0, index)

  private val ballotMapping: mutable.Map[Instance, Ballot] = mutable.Map()

  private val executedCommands: mutable.Set[Command] = mutable.Set()

  var commands: mutable.Map[Instance, State] = mutable.Map[Instance, State]()

  var conflictsMap =
    mutable.Map[String, (mutable.Set[Instance], mutable.Set[Instance])]()

  val removeCommands: mutable.Set[Instance] = mutable.Set[Instance]()

  var graph: DependencyGraph = new DependencyGraph()

  // Methods ///////////////////////////////////////////////////////////////////
  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    import ReplicaInbound.Request
    inbound.request match {
      case Request.ClientRequest(r)  => handleClientRequest(src, r)
      case Request.PreAccept(r)      => handlePreAccept(src, r)
      case Request.PreAcceptOk(r)    => handlePreAcceptOk(src, r)
      case Request.Accept(r)         => handleAccept(src, r)
      case Request.AcceptOk(r)       => handleAcceptOk(src, r)
      case Request.Commit(r)         => handleCommit(src, r)
      case Request.PrepareRequest(r) => handlePrepareRequest(src, r)
      case Request.PrepareOk(r)      => handlePrepareOk(src, r)
      case Request.Nack(r)           => handleNack(src, r)
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  // TODO(mwhittaker): This method has to add 1 to the maximum sequence number.
  private def findAttributes(
      command: Command,
      commandInstance: Instance
  ): (mutable.Set[Instance], Int) = {
    var deps: mutable.Set[Instance] = mutable.Set()
    var maxSeqNum: Int = 0

    val commandString = command.command.toStringUtf8
    val tokens = commandString.split(" ")

    val action = tokens(0)
    val key = tokens(1)

    val tupleSet: (mutable.Set[Instance], mutable.Set[Instance]) =
      conflictsMap.getOrElse(key,
                             (mutable.Set[Instance](), mutable.Set[Instance]()))

    action match {
      case "GET" => deps = deps.union(tupleSet._2)
      case "SET" => deps = deps.union(tupleSet._1).union(tupleSet._2)
    }

    if (deps.isEmpty) {
      return (deps, 0)
    }

    // TODO(mwhittaker): This doesn't compute the maximum sequence number, I
    // don't think.
    val state = commands.get(deps.head)
    if (state.nonEmpty) {
      maxSeqNum = Math.max(maxSeqNum, state.get.sequenceNumber)
    }
    deps.remove(commandInstance)
    (deps, maxSeqNum)
  }

  private def updateConflictMap(command: Command, instance: Instance): Unit = {
    val commandString = command.command.toStringUtf8
    val tokens = commandString.split(" ")

    val action = tokens(0)
    val key = tokens(1)

    val tupleSet: (mutable.Set[Instance], mutable.Set[Instance]) =
      conflictsMap.getOrElse(key,
                             (mutable.Set[Instance](), mutable.Set[Instance]()))

    action match {
      case "GET" => tupleSet._1.add(instance)
      case "SET" => tupleSet._2.add(instance)
    }

    conflictsMap.put(key, tupleSet)
    logger.info("Tuple set: " + tupleSet.toString())
  }

  private def handleClientRequest(
      address: Transport#Address,
      request: Request
  ): Unit = {
    val instance: Instance = Instance(index, nextAvailableInstance)
    nextAvailableInstance += 1

    instanceClientMapping.put(instance, address)
    ballotMapping.put(instance, currentBallot)
    updateConflictMap(request.command, instance)

    val attributes = findAttributes(request.command, instance)
    val seqCommand: Int = attributes._2
    val seqDeps: mutable.Set[Instance] = attributes._1

    commands.put(
      instance,
      State(request.command, seqCommand, seqDeps, PreAccepted)
    )

    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != config.fastQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(
          ReplicaInbound().withPreAccept(
            PreAccept(
              command = request.command,
              sequenceNumber = seqCommand,
              dependencies = seqDeps.toSeq,
              commandInstance = instance,
              avoid = false,
              ballot = ballotMapping.getOrElse(instance, currentBallot)
            )
          )
        )
        count += 1
      }
      setIndex += 1
    }

    val buffer =
      preacceptResponses.getOrElse(instance, mutable.Buffer[PreAcceptOk]())
    val message = PreAcceptOk(request.command,
                              seqCommand,
                              seqDeps.toSeq,
                              instance,
                              false,
                              ballotMapping.getOrElse(instance, currentBallot))
    buffer.append(message)
    preacceptResponses.put(instance, buffer)
  }

  private def startPhaseOne(
      instance: Instance,
      newCommand: Command,
      value: PrepareOk
  ): Unit = {
    val attributes = findAttributes(newCommand, value.instance)
    val seqCommand: Int = attributes._2
    val seqDeps: mutable.Set[Instance] = attributes._1
    commands.put(
      instance,
      State(newCommand, seqCommand, seqDeps, PreAccepted)
    )
    ballotMapping.put(instance, value.ballot)

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
              ballot = ballotMapping.getOrElse(instance, value.ballot)
            )
          )
        )
        count += 1
      }
      setIndex += 1
    }

    val buffer =
      preacceptResponses.getOrElse(instance, mutable.Buffer[PreAcceptOk]())
    val message = PreAcceptOk(value.command,
                              seqCommand,
                              seqDeps.toSeq,
                              instance,
                              true,
                              ballotMapping.getOrElse(instance, currentBallot))
    buffer.append(message)
    preacceptResponses.put(instance, buffer)
  }

  private def handlePreAccept(
      address: Transport#Address,
      value: PreAccept
  ): Unit = {
    if (value.ballot.ordering < ballotMapping
          .getOrElse(value.commandInstance, lowestBallot)
          .ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          NACK(
            oldBallot =
              ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }

    val attributes = findAttributes(value.command, value.commandInstance)
    val maxSequence: Int = attributes._2
    //sequenceNumbers.put(value.command, Math.max(maxSequence + 1, sequenceNumbers.getOrElse(value.command, 1)))
    var seqNum: Int = value.sequenceNumber
    seqNum = Math.max(seqNum, maxSequence + 1)

    val depsLocal: mutable.Set[Instance] = attributes._1
    var deps: mutable.Set[Instance] = mutable.Set(value.dependencies: _*)
    deps = deps.union(depsLocal)

    commands.put(value.commandInstance,
                 State(value.command, seqNum, deps, PreAccepted))
    updateConflictMap(value.command, value.commandInstance)
    ballotMapping.put(value.commandInstance, value.ballot)

    val leader = replicas(value.commandInstance.leaderIndex)
    leader.send(
      ReplicaInbound().withPreAcceptOk(
        PreAcceptOk(
          value.command,
          seqNum,
          deps.toSeq,
          value.commandInstance,
          value.avoid,
          ballotMapping.getOrElse(value.commandInstance, value.ballot)
        )
      )
    )
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

  private def handlePreAcceptOk(
      address: Transport#Address,
      value: PreAcceptOk
  ): Unit = {
    // TODO(mwhittaker): Understand when we NACK. NACKing a PreAccept makes
    // sense to me, byt why NACK a PreAcceptOk?
    if (value.ballot.ordering < ballotMapping
          .getOrElse(value.commandInstance, lowestBallot)
          .ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          NACK(
            oldBallot =
              ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }
    ballotMapping.put(value.commandInstance, value.ballot)

    // TODO(mwhittaker): Check that we're getting PreAcceptOks in our current
    // round and not in a higher round.

    // Verify leader is receiving responses
    if (value.commandInstance.leaderIndex != index) {
      // TODO(mwhittaker): Add fatal message.
      return
    }

    // TODO(mwhittaker): Handle fast path. Right now, I think this always goes
    // to the slow path.
    val preAcceptOkSet: mutable.Buffer[PreAcceptOk] = preacceptResponses
      .getOrElse(value.commandInstance, mutable.Buffer.empty)
    preAcceptOkSet.append(value)
    preacceptResponses.put(value.commandInstance, preAcceptOkSet)

    if (preacceptResponses
          .getOrElse(value.commandInstance, mutable.Buffer.empty)
          .size < config.slowQuorumSize) {
      // TODO(mwhittaker): Add a debug log maybe?
      return
    }

    val N: Int = config.replicaAddresses.size
    val seqTuple: (Boolean, Option[Int]) = checkSameSequenceNumbers(
      preacceptResponses.getOrElse(value.commandInstance, mutable.Buffer.empty),
      N - 1
    )
    val depsTuple: (Boolean, Option[Seq[Instance]]) = checkSameDependencies(
      preacceptResponses.getOrElse(value.commandInstance, mutable.Buffer.empty),
      N - 1
    )

    if (seqTuple._1 && depsTuple._1 && !value.avoid) {
      // Send request reply to client
      val clientAddress: Option[Transport#Address] =
        instanceClientMapping.get(value.commandInstance)
      // TODO(mwhittaker): Double check that even backup replicas have the
      // address of the client.
      if (clientAddress.nonEmpty) {
        val client =
          chan[Client[Transport]](clientAddress.get, Client.serializer)
        // TODO(mwhittaker): Double check that we're sending back the right
        // command and instance.
        client.send(
          ClientInbound().withRequestReply(
            RequestReply(
              value.command,
              value.commandInstance
            )
          )
        )
      }

      // Run commit phase
      runCommitPhase(value.command,
                     seqTuple._2.get,
                     depsTuple._2.get,
                     value.commandInstance)
    } else {
      var updatedDeps: mutable.Set[Instance] = mutable.Set()
      var maxSequenceNumber: Int = value.sequenceNumber
      for (preacceptResponse <- preacceptResponses.getOrElse(
             value.commandInstance,
             mutable.Buffer.empty
           )) {
        updatedDeps =
          updatedDeps.union(mutable.Set(preacceptResponse.dependencies: _*))
        maxSequenceNumber =
          Math.max(maxSequenceNumber, preacceptResponse.sequenceNumber)
      }
      startPaxosAcceptPhase(value.command,
                            maxSequenceNumber,
                            updatedDeps,
                            value.commandInstance)
    }
  }

  private def startPaxosAcceptPhase(
      command: Command,
      sequenceNum: Int,
      deps: mutable.Set[Instance],
      commandInstance: Instance
  ): Unit = {
    commands.put(commandInstance, State(command, sequenceNum, deps, Accepted))
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
              ballotMapping.getOrElse(commandInstance, lowestBallot)
            )
          )
        )
        count += 1
      }
      setIndex += 1
    }

    // Add leader accept ok response
    val buffer =
      acceptOkResponses.getOrElse(commandInstance, mutable.Buffer[AcceptOk]())
    val message = AcceptOk(
      command,
      commandInstance,
      ballotMapping.getOrElse(commandInstance, lowestBallot)
    )
    buffer.append(message)
    acceptOkResponses.put(commandInstance, buffer)
  }

  private def handleAccept(src: Transport#Address, value: Accept): Unit = {
    if (value.ballot.ordering < ballotMapping
          .getOrElse(value.commandInstance, lowestBallot)
          .ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          NACK(
            oldBallot =
              ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }

    commands.put(value.commandInstance,
                 State(value.command,
                       value.sequenceNumber,
                       mutable.Set(value.dependencies: _*),
                       Accepted))
    ballotMapping.put(value.commandInstance, value.ballot)
    val leader = replicas(value.commandInstance.leaderIndex)
    leader.send(
      ReplicaInbound().withAcceptOk(
        AcceptOk(
          command = value.command,
          commandInstance = value.commandInstance,
          ballot = ballotMapping.getOrElse(value.commandInstance, value.ballot)
        )
      )
    )
  }

  private def handleAcceptOk(
      src: Transport#Address,
      value: AcceptOk
  ): Unit = {
    // TODO(mwhittaker): Double check that NACKing here is appropriate.
    if (value.ballot.ordering < ballotMapping
          .getOrElse(value.commandInstance, lowestBallot)
          .ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          NACK(
            oldBallot =
              ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }

    val responseSet: mutable.Buffer[AcceptOk] = acceptOkResponses.getOrElse(
      value.commandInstance,
      mutable.Buffer.empty
    )
    responseSet.append(value)
    acceptOkResponses.put(value.commandInstance, responseSet)
    ballotMapping.put(value.commandInstance, value.ballot)

    if (responseSet.size >= config.slowQuorumSize) {
      // send request reply to client
      val clientAddress: Option[Transport#Address] =
        instanceClientMapping.get(value.commandInstance)
      // TODO(mwhittaker): Make sure backup replicas have client address.
      if (clientAddress.nonEmpty) {
        val client =
          chan[Client[Transport]](clientAddress.get, Client.serializer)
        client.send(
          ClientInbound().withRequestReply(
            RequestReply(
              value.command,
              value.commandInstance
            )
          )
        )
      }
      // run commit phase
      val commandState: Option[State] = commands.get(value.commandInstance)
      if (commandState.nonEmpty) {
        runCommitPhase(commandState.get.command,
                       commandState.get.sequenceNumber,
                       commandState.get.dependencies.toSeq,
                       value.commandInstance)
      }
    }
  }

  // TODO(mwhittaker): Is this correct? Can we remove instances this eagerly?
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
    commands.put(
      instance,
      State(command, seqNum, mutable.Set(deps: _*), Committed)
    )

    // TODO(mwhittaker): I think this is adding committed commands before the
    // command's dependencies are necessarily committed.
    recursiveAdd(instance)
    graph.executeDependencyGraph(stateMachine, executedCommands)

    for (inst <- removeCommands) {
      val st = commands.get(inst)
      if (st.nonEmpty) {
        removeInstance(st.get.command, inst)
      }
      commands.remove(inst)
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
            ballotMapping.getOrElse(instance, lowestBallot)
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
      val headState = commands.get(headInstance)

      if (headState.nonEmpty && headState.get.status == Committed) {
        graph.directedGraph.addVertex(
          (headState.get.command, headState.get.sequenceNumber)
        )
        removeCommands.add(headInstance)

        for (dep <- headState.get.dependencies) {
          val depState = commands.get(dep)
          if (depState.nonEmpty && depState.get.status == Committed) {
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
    // TODO(mwhittaker): You can't NACK a commit can you?
    if (value.ballot.ordering < ballotMapping
          .getOrElse(value.commandInstance, lowestBallot)
          .ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(
        ReplicaInbound().withNack(
          NACK(
            oldBallot =
              ballotMapping.getOrElse(value.commandInstance, lowestBallot),
            instance = value.commandInstance
          )
        )
      )
      return
    }

    // TODO(mwhittaker): Call runCommitPhase here instead?

    commands.put(value.commandInstance,
                 State(value.command,
                       value.sequenceNumber,
                       mutable.Set(value.dependencies: _*),
                       Committed))

    recursiveAdd(value.commandInstance)
    graph.executeDependencyGraph(stateMachine, executedCommands)

    for (inst <- removeCommands) {
      val st = commands.get(inst)
      if (st.nonEmpty) {
        removeInstance(st.get.command, inst)
      }
      commands.remove(inst)
    }
    removeCommands.clear()
  }

  private def explicitPrepare(instance: Instance, oldBallot: Ballot): Unit = {
    val newBallot: Ballot = Ballot(oldBallot.ordering + 1, index)
    currentBallot = newBallot
    ballotMapping.put(instance, currentBallot)
    for (replica <- replicas) {
      replica.send(
        ReplicaInbound().withPrepareRequest(
          Prepare(
            ballot = newBallot,
            instance
          )
        )
      )
    }
  }

  private def handleNack(src: Transport#Address, value: NACK): Unit = {
    if (index == value.instance.leaderIndex) {
      explicitPrepare(value.instance, value.oldBallot)
    }
  }

  private def handlePrepareRequest(
      src: Transport#Address,
      value: Prepare
  ): Unit = {
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    if (value.ballot.ordering > ballotMapping
          .getOrElse(value.instance, lowestBallot)
          .ordering) {
      ballotMapping.put(value.instance, value.ballot)
      val state: Option[State] = commands.get(value.instance)
      if (state.nonEmpty) {
        replica.send(
          ReplicaInbound().withPrepareOk(
            PrepareOk(
              ballot = value.ballot,
              instance = value.instance,
              command = state.get.command,
              sequenceNumber = state.get.sequenceNumber,
              dependencies = state.get.dependencies.toSeq,
              status = state.get.status.toString,
              replicaIndex = index
            )
          )
        )
      }
      // TODO(mwhittaker): What if the state is non-empty?
    } else {
      replica.send(
        ReplicaInbound().withNack(
          NACK(
            oldBallot = ballotMapping.getOrElse(value.instance, lowestBallot),
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
      if (response.instance.leaderIndex != replicaIndex) {
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
        checkPrepareOkSame(config.n / 2, value.instance.leaderIndex, R)
      if (R.exists(_.status.equals(Committed.toString))) {
        val message: PrepareOk =
          R.filter(_.status.eq(Committed.toString)).head
        runCommitPhase(message.command,
                       message.sequenceNumber,
                       message.dependencies,
                       message.instance)
      } else if (R.exists(_.status.equals(Accepted.toString))) {
        val message: PrepareOk =
          R.filter(_.status.equals(Accepted.toString)).head
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
