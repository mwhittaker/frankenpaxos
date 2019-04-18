package frankenpaxos.epaxos

import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.scalajs.js.annotation._
import frankenpaxos._
import frankenpaxos.statemachine._
import java.io._

import scala.collection.mutable.ListBuffer

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
      yield
        chan[Replica[Transport]](
          replicaAddress,
          Replica.serializer
        )

  var nextAvailableInstance: Int = 1

  private val fastQuorumSize: Int = replicas.size - 1

  private val slowQuorumSize: Int = (replicas.size / 2) + 1

  object ReplicaStatus extends Enumeration {
    type ReplicaStatus = Value
    val PreAccepted = Value("PreAccepted")
    val Accepted = Value("Accepted")
    val Committed = Value("Committed")
    val Executed = Value("Executed")
  }
  import ReplicaStatus._

  case class State(
      command: Command,
      sequenceNumber: Int,
      dependencies: mutable.Set[Instance],
      status: ReplicaStatus
  )

  // The preaccept responses
  var preacceptResponses
    : mutable.Map[Instance, mutable.ListBuffer[PreAcceptOk]] = mutable.Map()

  // The accept ok responses
  var acceptOkResponses: mutable.Map[Instance, mutable.ListBuffer[AcceptOk]] =
    mutable.Map()

  var prepareResponses: mutable.Map[Instance, mutable.ListBuffer[PrepareOk]] =
    mutable.Map()

  private val instanceClientMapping: mutable.Map[Instance, Transport#Address] =
    mutable.Map()

  val stateMachine: KeyValueStore = new KeyValueStore()

  private val lowestBallot: Ballot = Ballot(0, index)
  private var currentBallot: Ballot = Ballot(0, index)

  private val ballotMapping: mutable.Map[Instance, Ballot] = mutable.Map()

  private val executedCommands: mutable.Set[Command] = mutable.Set()

  var commands: mutable.Map[Instance, State] = mutable.Map[Instance, State]()
  var conflictsMap
    : mutable.Map[String, (mutable.Set[Instance], mutable.Set[Instance])] =
    mutable.Map[String, (mutable.Set[Instance], mutable.Set[Instance])]()
  val removeCommands: mutable.Set[Instance] = mutable.Set[Instance]()
  var graph: DependencyGraph = new DependencyGraph()

  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    import ReplicaInbound.Request
    inbound.request match {
      case Request.ClientRequest(r)  => handleClientRequest(src, r)
      case Request.PreAccept(r)      => handlePreAccept(src, r)
      case Request.PreAcceptOk(r)    => handlePreAcceptOk(src, r)
      case Request.Accept(r)         => handlePaxosAccept(src, r)
      case Request.AcceptOk(r)       => handlePaxosAcceptOk(src, r)
      case Request.Commit(r)         => handleCommit(src, r)
      case Request.PrepareRequest(r) => handleExplicitPrepare(src, r)
      case Request.PrepareOk(r)      => handleExplicitPrepareOk(src, r)
      case Request.Nack(r)           => handleNack(src, r)
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

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

    if (deps.isEmpty) return (deps, 0)
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
      value: Request
  ): Unit = {
    val instance: Instance = Instance(index, nextAvailableInstance)

    instanceClientMapping.put(instance, address)
    ballotMapping.put(instance, currentBallot)
    updateConflictMap(value.command, instance)

    nextAvailableInstance += 1

    val attributes = findAttributes(value.command, instance)
    val seqCommand: Int = attributes._2
    val seqDeps: mutable.Set[Instance] = attributes._1

    commands.put(
      instance,
      State(value.command, seqCommand, seqDeps, ReplicaStatus.PreAccepted)
    )

    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != fastQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(
          ReplicaInbound().withPreAccept(
            PreAccept(
              command = value.command,
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
      preacceptResponses.getOrElse(instance, ListBuffer[PreAcceptOk]())
    val message = PreAcceptOk(value.command,
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
      State(newCommand, seqCommand, seqDeps, ReplicaStatus.PreAccepted)
    )
    ballotMapping.put(instance, value.ballot)

    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != fastQuorumSize - 1) {
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
      preacceptResponses.getOrElse(instance, ListBuffer[PreAcceptOk]())
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
                 State(value.command, seqNum, deps, ReplicaStatus.PreAccepted))
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
      preAcceptResponses: mutable.ListBuffer[PreAcceptOk],
      numSameNeeded: Int,
      total: Int
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
      preAcceptResponses: mutable.ListBuffer[PreAcceptOk],
      numSameNeeded: Int,
      total: Int
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

    // Verify leader is receiving responses
    if (value.commandInstance.leaderIndex == index) {
      //preacceptResponses.put(value.commandInstance, preacceptResponses.getOrElse(value.commandInstance, 0) + 1)
      val preAcceptOkSet: mutable.ListBuffer[PreAcceptOk] = preacceptResponses
        .getOrElse(value.commandInstance, mutable.ListBuffer.empty)
      preAcceptOkSet.append(value)
      preacceptResponses.put(value.commandInstance, preAcceptOkSet)
      val quorumSize: Int = (config.replicaAddresses.size / 2) + 1
      //println(value + " " + preacceptResponses)
      if (preacceptResponses
            .getOrElse(value.commandInstance, mutable.ListBuffer.empty)
            .size >= quorumSize) {
        //println("Reached quorum size")
        val N: Int = config.replicaAddresses.size
        val seqTuple: (Boolean, Option[Int]) = checkSameSequenceNumbers(
          preacceptResponses.getOrElse(value.commandInstance,
                                       mutable.ListBuffer.empty),
          N - 1,
          N
        )
        val depsTuple: (Boolean, Option[Seq[Instance]]) = checkSameDependencies(
          preacceptResponses.getOrElse(value.commandInstance,
                                       mutable.ListBuffer.empty),
          N - 1,
          N
        )
        /*val sameSequenceNumber: Boolean = checkSameSequenceNumbers(preacceptResponses.getOrElse(value.commandInstance, mutable.Set.empty), N - 2, N)
        val sameDeps: Boolean = checkSameDependencies(preacceptResponses.getOrElse(value.commandInstance, mutable.Set.empty), N - 2, N)*/
        if (seqTuple._1 && depsTuple._1 && !value.avoid) {
          // Send request reply to client
          val clientAddress: Option[Transport#Address] =
            instanceClientMapping.get(value.commandInstance)
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
          if (value.dependencies.nonEmpty) {
            println(value.dependencies)
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
                 mutable.ListBuffer.empty
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
    }
  }

  private def startPaxosAcceptPhase(
      command: Command,
      sequenceNum: Int,
      deps: mutable.Set[Instance],
      commandInstance: Instance
  ): Unit = {
    commands.put(commandInstance,
                 State(command, sequenceNum, deps, ReplicaStatus.Accepted))
    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != slowQuorumSize - 1) {
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
      acceptOkResponses.getOrElse(commandInstance, ListBuffer[AcceptOk]())
    val message = AcceptOk(
      command,
      commandInstance,
      ballotMapping.getOrElse(commandInstance, lowestBallot)
    )
    buffer.append(message)
    acceptOkResponses.put(commandInstance, buffer)
  }

  private def handlePaxosAccept(src: Transport#Address, value: Accept): Unit = {
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
                       ReplicaStatus.Accepted))
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

  private def handlePaxosAcceptOk(
      src: Transport#Address,
      value: AcceptOk
  ): Unit = {
    val start_time = java.time.Instant.now()
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

    val responseSet: mutable.ListBuffer[AcceptOk] = acceptOkResponses.getOrElse(
      value.commandInstance,
      mutable.ListBuffer.empty
    )
    responseSet.append(value)
    acceptOkResponses.put(value.commandInstance, responseSet)
    ballotMapping.put(value.commandInstance, value.ballot)

    val numResponsesNeeded: Int = replicas.size / 2
    if (responseSet.size >= numResponsesNeeded + 1) {
      // send request reply to client
      val clientAddress: Option[Transport#Address] =
        instanceClientMapping.get(value.commandInstance)
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
      State(command, seqNum, mutable.Set(deps: _*), ReplicaStatus.Committed)
    )

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

      if (headState.nonEmpty && headState.get.status == ReplicaStatus.Committed) {
        graph.directedGraph.addVertex(
          (headState.get.command, headState.get.sequenceNumber)
        )
        removeCommands.add(headInstance)

        for (dep <- headState.get.dependencies) {
          val depState = commands.get(dep)
          if (depState.nonEmpty && depState.get.status == ReplicaStatus.Committed) {
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
                       ReplicaStatus.Committed))

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

  private def handleExplicitPrepare(
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
      responses: ListBuffer[PrepareOk]
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

  private def handleExplicitPrepareOk(
      src: Transport#Address,
      value: PrepareOk
  ): Unit = {
    val prepareValues: ListBuffer[PrepareOk] =
      prepareResponses.getOrElse(value.instance, ListBuffer.empty)
    prepareValues.append(value)
    val quorumSize: Int = (config.n / 2) + 1
    if (prepareValues.size >= quorumSize) {
      val maxBallot: Int =
        prepareValues.maxBy(_.ballot.ordering).ballot.ordering
      val R: ListBuffer[PrepareOk] =
        prepareValues.filter(_.ballot.ordering == maxBallot)
      val sameMessage: (Boolean, PrepareOk, Boolean, PrepareOk) =
        checkPrepareOkSame(config.n / 2, value.instance.leaderIndex, R)
      if (R.exists(_.status.equals(ReplicaStatus.Committed.toString))) {
        val message: PrepareOk =
          R.filter(_.status.eq(ReplicaStatus.Committed.toString)).head
        runCommitPhase(message.command,
                       message.sequenceNumber,
                       message.dependencies,
                       message.instance)
      } else if (R.exists(_.status.equals(ReplicaStatus.Accepted.toString))) {
        val message: PrepareOk =
          R.filter(_.status.equals(ReplicaStatus.Accepted.toString)).head
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
