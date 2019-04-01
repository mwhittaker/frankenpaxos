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

  case class State(command: Command, sequenceNumber: Int, dependencies: mutable.Set[Instance], status: ReplicaStatus)

  // Each replica's commands private state
  //var commands: mutable.Map[Instance, State] = mutable.Map()

  // The preaccept responses
  var preacceptResponses: mutable.Map[Instance, mutable.ListBuffer[PreAcceptOk]] = mutable.Map()

  // The accept ok responses
  var acceptOkResponses: mutable.Map[Instance, mutable.ListBuffer[AcceptOk]] = mutable.Map()

  var prepareResponses: mutable.Map[Instance, mutable.ListBuffer[PrepareOk]] = mutable.Map()

  private val instanceClientMapping: mutable.Map[Instance, Transport#Address] = mutable.Map()

  //val stateMachine: StateMachine = new StateMachine()
  val stateMachine: KeyValueStore = new KeyValueStore()

  private val lowestBallot: Ballot = Ballot(0, index)
  private var currentBallot: Ballot = Ballot(0, index)

  private val ballotMapping: mutable.Map[Instance, Ballot] = mutable.Map()

  private val executedCommands: mutable.Set[Command] = mutable.Set()

  object InstanceOrdering extends Ordering[Instance] {
    def compare(a: Instance, b: Instance) = -1 * a.instanceNumber.compare(b.instanceNumber)
  }
  var commands: mutable.TreeMap[Instance, State] = mutable.TreeMap[Instance, State]()(InstanceOrdering)

  val pw = new PrintWriter(new File("latencies.txt" ))

  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    import ReplicaInbound.Request
    inbound.request match {
      case Request.ClientRequest(r) => handleClientRequest(src, r)
      case Request.PreAccept(r) => handlePreAccept(src, r)
      case Request.PreAcceptOk(r) => handlePreAcceptOk(src, r)
      case Request.Accept(r) => handlePaxosAccept(src, r)
      case Request.AcceptOk(r) => handlePaxosAcceptOk(src, r)
      case Request.Commit(r) => handleCommit(src, r)
      case Request.PrepareRequest(r) => handleExplicitPrepare(src, r)
      case Request.PrepareOk(r) => handleExplicitPrepareOk(src, r)
      case Request.Nack(r) => handleNack(src, r)
      case Request.Read(r) => handleRead(src, r)
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  private def handleRead(src: Transport#Address, r: Read): Unit = {
    logger.info("Read handler")
    executeGraph(buildDependencyGraph(r.instance))
    logger.info("After graph execution")

    /*
    if (index == r.instance.leaderIndex) {
      for (replica <- replicas) {
        if (config.replicaAddresses.indexOf(replica) != index) {
          replica.send(ReplicaInbound().withRead(r))
        }
      }
    }*/

    val client = chan[Client[Transport]](src, Client.serializer)
    //if (index == r.instance.leaderIndex) {
     client.send(ClientInbound().withReadReply(ReadReply(
      state = stateMachine.toString(),
      command = r.command
      )))
    //}
  }

  private def findSequenceNumber(command: Command): Int = {
    var maxSequenceNum: Int = 0
    for (instance <- commands.keys) {
      val state: Option[State] = commands.get(instance)
      if (state.nonEmpty) {
        if (stateMachine.conflicts(command.command.toByteArray, state.get.command.command.toByteArray)) {
          maxSequenceNum = Math.max(maxSequenceNum, state.get.sequenceNumber)
        }
      }
    }

    maxSequenceNum + 1
  }

  private def findDependencies(command: Command, commandInstance: Instance): mutable.Set[Instance] = {
    val deps: mutable.Set[Instance] = mutable.Set()
    val maxDeps: Int = config.n
    var currNum: Int = 0

    for ((instance, _) <- commands) {
      if (currNum == maxDeps) return deps
      val state: Option[State] = commands.get(instance)
      if (state.nonEmpty) {
        val condition = instance.equals(commandInstance)
        if (!condition && checkConflicts(command.command, state.get.command.command)) {
          deps.add(instance)
          currNum += 1
          logger.info("Conflict")
        }
      }
    }
    deps
  }

  private def checkConflicts(firstCommand: ByteString, secondCommand: ByteString): Boolean = {
    val firstCommandString = firstCommand.toStringUtf8
    val firstTokens = firstCommandString.split(" ")
    var firstInput: Input = null

    val secondCommandString = secondCommand.toStringUtf8
    val secondTokens = secondCommandString.split(" ")
    var secondInput: Input = null


    firstTokens(0) match {
      case "GET" => firstInput = Input().withGetRequest(GetRequest(Seq(firstTokens(1))))
      case "SET" => firstInput = Input().withSetRequest(SetRequest(Seq(SetKeyValuePair(firstTokens(1), firstTokens(2)))))
    }

    secondTokens(0) match {
      case "GET" => secondInput = Input().withGetRequest(GetRequest(Seq(secondTokens(1))))
      case "SET" => secondInput = Input().withSetRequest(SetRequest(Seq(SetKeyValuePair(secondTokens(1), secondTokens(2)))))
    }

    val result = stateMachine.typedConflicts(firstInput, secondInput)
    logger.info("Command conflicts: " + stateMachine.debug)
    result
  }

  private def handleClientRequest(address: Transport#Address, value: Request): Unit = {
    val start_time = java.time.Instant.now()
    val instance: Instance = Instance(index, nextAvailableInstance)
    instanceClientMapping.put(instance, address)
    ballotMapping.put(instance, currentBallot)
    nextAvailableInstance += 1

    val seqCommand: Int = findSequenceNumber(value.command)
    val seqDeps: mutable.Set[Instance] = findDependencies(value.command, instance)
    commands.put(instance, State(value.command, seqCommand, seqDeps, ReplicaStatus.PreAccepted))

    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != fastQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(ReplicaInbound().withPreAccept(PreAccept(
          command = value.command,
          sequenceNumber = seqCommand,
          dependencies = seqDeps.toSeq,
          commandInstance = instance,
          avoid = false,
          ballot = ballotMapping.getOrElse(instance, currentBallot)
        )))
        count += 1
      }
      setIndex += 1
    }

    // Send to command leader
    replicas(index).send(ReplicaInbound().withPreAccept(PreAccept(
      command = value.command,
      sequenceNumber = seqCommand,
      dependencies = seqDeps.toSeq,
      commandInstance = instance,
      avoid = false,
      ballot = ballotMapping.getOrElse(instance, currentBallot)
    )))

    val stop_time = java.time.Instant.now()
    val duration = java.time.Duration.between(start_time, stop_time)
    logger.info("handleClientRequest: " + duration.toMillis)
  }

  private def startPhaseOne(instance: Instance, newCommand: Command, value: PrepareOk): Unit = {
    val seqCommand: Int = findSequenceNumber(newCommand)
    val seqDeps: mutable.Set[Instance] = findDependencies(newCommand, value.instance)
    commands.put(instance, State(newCommand, seqCommand, seqDeps, ReplicaStatus.PreAccepted))
    ballotMapping.put(instance, value.ballot)

    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != fastQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(ReplicaInbound().withPreAccept(PreAccept(
          command = newCommand,
          sequenceNumber = seqCommand,
          dependencies = seqDeps.toSeq,
          commandInstance = instance,
          avoid = true,
          ballot = ballotMapping.getOrElse(instance, value.ballot)
        )))
        count += 1
      }
      setIndex += 1
    }

    // Send to command leader
    replicas(index).send(ReplicaInbound().withPreAccept(PreAccept(
      command = newCommand,
      sequenceNumber = seqCommand,
      dependencies = seqDeps.toSeq,
      commandInstance = instance,
      avoid = true,
      ballot = ballotMapping.getOrElse(instance, value.ballot)
    )))
  }

  private def handlePreAccept(address: Transport#Address, value: PreAccept): Unit = {
    if (value.ballot.ordering < ballotMapping.getOrElse(value.commandInstance, lowestBallot).ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(ReplicaInbound().withNack(NACK(
        oldBallot = ballotMapping.getOrElse(value.commandInstance, lowestBallot),
        instance = value.commandInstance
      )))
      return
    }

    if (value.commandInstance.leaderIndex == index) {
      val leader = replicas(value.commandInstance.leaderIndex)
      leader.send(ReplicaInbound().withPreAcceptOk(
      PreAcceptOk(
        value.command,
        value.sequenceNumber,
        value.dependencies,
        value.commandInstance,
        value.avoid,
        ballotMapping.getOrElse(value.commandInstance, value.ballot)
      )
      ))
      return
    }

    val maxSequence: Int = findSequenceNumber(value.command)
    //sequenceNumbers.put(value.command, Math.max(maxSequence + 1, sequenceNumbers.getOrElse(value.command, 1)))
    var seqNum: Int = value.sequenceNumber
    seqNum = Math.max(seqNum, maxSequence + 1)

    val depsLocal: mutable.Set[Instance] = findDependencies(value.command, value.commandInstance)
    var deps: mutable.Set[Instance] = mutable.Set(value.dependencies:_*)
    deps = deps.union(depsLocal)
    //dependencies.put(value.command, mutable.Set(value.dependencies:_*).union(depsLocal))

    commands.put(value.commandInstance, State(value.command, seqNum, deps, ReplicaStatus.PreAccepted))
    ballotMapping.put(value.commandInstance, value.ballot)

    val leader = replicas(value.commandInstance.leaderIndex)
    leader.send(ReplicaInbound().withPreAcceptOk(
      PreAcceptOk(
        value.command,
        seqNum,
        deps.toSeq,
        value.commandInstance,
        value.avoid,
        ballotMapping.getOrElse(value.commandInstance, value.ballot)
      )
    ))
  }

  private def checkSameSequenceNumbers(preAcceptResponses: mutable.ListBuffer[PreAcceptOk], numSameNeeded: Int, total: Int): (Boolean, Option[Int]) = {
    val sequenceNumCount: mutable.Map[Int, Int] = mutable.Map()
    for (preacceptOk <- preAcceptResponses) {
      sequenceNumCount.put(preacceptOk.sequenceNumber, sequenceNumCount.getOrElse(preacceptOk.sequenceNumber, 0) + 1)
    }

    for (key <- sequenceNumCount.keys) {
      if (sequenceNumCount.getOrElse(key, 0) >= numSameNeeded) {
        return (true, Some(key))
      }
    }

    (false, None)
  }

  private def checkSameDependencies(preAcceptResponses: mutable.ListBuffer[PreAcceptOk], numSameNeeded: Int, total: Int): (Boolean, Option[Seq[Instance]]) = {
    val dependenciesNumCount: mutable.Map[Seq[Instance], Int] = mutable.Map()
    for (preacceptOk <- preAcceptResponses) {
      dependenciesNumCount.put(preacceptOk.dependencies, dependenciesNumCount.getOrElse(preacceptOk.dependencies, 0) + 1)
    }

    for (key <- dependenciesNumCount.keys) {
      if (dependenciesNumCount.getOrElse(key, 0) >= numSameNeeded) {
        return (true, Some(key))
      }
    }

    (false, None)
  }

  private def handlePreAcceptOk(address: Transport#Address, value: PreAcceptOk): Unit = {
    val start_time = java.time.Instant.now()
    if (value.ballot.ordering < ballotMapping.getOrElse(value.commandInstance, lowestBallot).ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(ReplicaInbound().withNack(NACK(
        oldBallot = ballotMapping.getOrElse(value.commandInstance, lowestBallot),
        instance = value.commandInstance
      )))
      return
    }
    ballotMapping.put(value.commandInstance, value.ballot)
    // Verify leader is receiving responses
    if (value.commandInstance.leaderIndex == index) {
      //preacceptResponses.put(value.commandInstance, preacceptResponses.getOrElse(value.commandInstance, 0) + 1)
      val preAcceptOkSet: mutable.ListBuffer[PreAcceptOk] = preacceptResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty)
      preAcceptOkSet.append(value)
      preacceptResponses.put(value.commandInstance, preAcceptOkSet)
      val quorumSize: Int = (config.replicaAddresses.size / 2) + 1
      //println(value + " " + preacceptResponses)
      if (preacceptResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty).size >= quorumSize) {
        //println("Reached quorum size")
        val N: Int = config.replicaAddresses.size
        val seqTuple: (Boolean, Option[Int]) = checkSameSequenceNumbers(
          preacceptResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty), N - 2, N)
        val depsTuple: (Boolean, Option[Seq[Instance]]) = checkSameDependencies(
          preacceptResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty), N - 2, N)
        /*val sameSequenceNumber: Boolean = checkSameSequenceNumbers(preacceptResponses.getOrElse(value.commandInstance, mutable.Set.empty), N - 2, N)
        val sameDeps: Boolean = checkSameDependencies(preacceptResponses.getOrElse(value.commandInstance, mutable.Set.empty), N - 2, N)*/
        if (seqTuple._1 && depsTuple._1 && !value.avoid) {
          // Send request reply to client
          val clientAddress: Option[Transport#Address] = instanceClientMapping.get(value.commandInstance)
          if (clientAddress.nonEmpty) {
            val client = chan[Client[Transport]](clientAddress.get, Client.serializer)
            client.send(ClientInbound().withRequestReply(RequestReply(
              value.command,
              value.commandInstance
            )))
          }
          if (value.dependencies.nonEmpty) {
           println(value.dependencies)
          }
          // Run commit phase
          runCommitPhase(value.command, seqTuple._2.get, depsTuple._2.get, value.commandInstance)
        } else {
          var updatedDeps: mutable.Set[Instance] = mutable.Set()
          var maxSequenceNumber: Int = value.sequenceNumber
          for (preacceptResponse <- preacceptResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty)) {
            updatedDeps = updatedDeps.union(mutable.Set(preacceptResponse.dependencies:_*))
            maxSequenceNumber = Math.max(maxSequenceNumber, preacceptResponse.sequenceNumber)
          }
          startPaxosAcceptPhase(value.command, maxSequenceNumber, updatedDeps, value.commandInstance)
        }
      }
    }
    val stop_time = java.time.Instant.now()
    val duration = java.time.Duration.between(start_time, stop_time)
    logger.info("handlePreAcceptOk: " + duration.toMillis)
  }

  private def startPaxosAcceptPhase(command: Command, sequenceNum: Int, deps: mutable.Set[Instance], commandInstance: Instance): Unit = {
    commands.put(commandInstance, State(command, sequenceNum, deps, ReplicaStatus.Accepted))
    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != slowQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(ReplicaInbound().withAccept(Accept(
          command,
          sequenceNum,
          deps.toSeq,
          commandInstance,
          ballotMapping.getOrElse(commandInstance, lowestBallot)
        )))
        count += 1
      }
      setIndex += 1
    }

    // Send to command leader
    replicas(index).send(ReplicaInbound().withAccept(Accept(
      command,
      sequenceNum,
      deps.toSeq,
      commandInstance,
      ballotMapping.getOrElse(commandInstance, lowestBallot)
    )))
  }

  private def handlePaxosAccept(src: Transport#Address, value: Accept): Unit = {
    val start_time = java.time.Instant.now()
    if (value.ballot.ordering < ballotMapping.getOrElse(value.commandInstance, lowestBallot).ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(ReplicaInbound().withNack(NACK(
        oldBallot = ballotMapping.getOrElse(value.commandInstance, lowestBallot),
        instance = value.commandInstance
      )))
      return
    }

    commands.put(value.commandInstance, State(value.command, value.sequenceNumber, mutable.Set(value.dependencies:_*), ReplicaStatus.Accepted))
    ballotMapping.put(value.commandInstance, value.ballot)
    val leader = replicas(value.commandInstance.leaderIndex)
    leader.send(ReplicaInbound().withAcceptOk(AcceptOk(
      command = value.command,
      commandInstance = value.commandInstance,
      ballot = ballotMapping.getOrElse(value.commandInstance, value.ballot)
    )))
    val stop_time = java.time.Instant.now()
    val duration = java.time.Duration.between(start_time, stop_time)
    logger.info("handlePaxosAccept: " + duration.toMillis)
  }

  private def handlePaxosAcceptOk(src: Transport#Address, value: AcceptOk): Unit = {
    val start_time = java.time.Instant.now()
    if (value.ballot.ordering < ballotMapping.getOrElse(value.commandInstance, lowestBallot).ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(ReplicaInbound().withNack(NACK(
        oldBallot = ballotMapping.getOrElse(value.commandInstance, lowestBallot),
        instance = value.commandInstance
      )))
      return
    }

    val responseSet: mutable.ListBuffer[AcceptOk] = acceptOkResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty)
    responseSet.append(value)
    acceptOkResponses.put(value.commandInstance, responseSet)
    ballotMapping.put(value.commandInstance, value.ballot)

    val numResponsesNeeded: Int = replicas.size / 2
    if (responseSet.size >= numResponsesNeeded) {
      // send request reply to client
      val clientAddress: Option[Transport#Address] = instanceClientMapping.get(value.commandInstance)
      if (clientAddress.nonEmpty) {
        val client = chan[Client[Transport]](clientAddress.get, Client.serializer)
        client.send(ClientInbound().withRequestReply(RequestReply(
          value.command,
          value.commandInstance
        )))
      }
      // run commit phase
      val commandState: Option[State] = commands.get(value.commandInstance)
      if (commandState.nonEmpty) {
        runCommitPhase(commandState.get.command, commandState.get.sequenceNumber, commandState.get.dependencies.toSeq, value.commandInstance)
      }
    }

    val stop_time = java.time.Instant.now()
    val duration = java.time.Duration.between(start_time, stop_time)
    logger.info("handlePaxosAcceptOk: " + duration.toMillis)
  }

  private def runCommitPhase(command: Command, seqNum: Int, deps: Seq[Instance], instance: Instance): Unit = {
    commands.put(instance, State(command, seqNum, mutable.Set(deps:_*), ReplicaStatus.Committed))
    //executeGraph(buildDependencyGraph(instance))
    //commands.remove(instance)
    for (replica <- replicas) {
      replica.send(ReplicaInbound().withCommit(Commit(
        command,
        seqNum,
        deps,
        instance,
        ballotMapping.getOrElse(instance, lowestBallot)
      )))
    }

  }

  private def handleCommit(src: Transport#Address, value: Commit): Unit = {
    if (value.ballot.ordering < ballotMapping.getOrElse(value.commandInstance, lowestBallot).ordering) {
      val replica = chan[Replica[Transport]](address, Replica.serializer)
      replica.send(ReplicaInbound().withNack(NACK(
        oldBallot = ballotMapping.getOrElse(value.commandInstance, lowestBallot),
        instance = value.commandInstance
      )))
      return
    }

    //commands.remove(value.commandInstance)
    commands.put(value.commandInstance, State(value.command, value.sequenceNumber, mutable.Set(value.dependencies:_*), ReplicaStatus.Committed))
    //executeGraph(buildDependencyGraph(value.commandInstance))
  }

  private def explicitPrepare(instance: Instance, oldBallot: Ballot): Unit = {
    val newBallot: Ballot = Ballot(oldBallot.ordering + 1, index)
    currentBallot = newBallot
    ballotMapping.put(instance, currentBallot)
    for (replica <- replicas) {
      replica.send(ReplicaInbound().withPrepareRequest(Prepare(
        ballot = newBallot,
        instance
      )))
    }
  }

  private def handleNack(src: Transport#Address, value: NACK): Unit = {
    if (index == value.instance.leaderIndex) {
      explicitPrepare(value.instance, value.oldBallot)
    }
  }

  private def handleExplicitPrepare(src: Transport#Address, value: Prepare): Unit = {
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    if (value.ballot.ordering > ballotMapping.getOrElse(value.instance, lowestBallot).ordering) {
      ballotMapping.put(value.instance, value.ballot)
      val state: Option[State] = commands.get(value.instance)
      if (state.nonEmpty) {
        replica.send(ReplicaInbound().withPrepareOk(PrepareOk(
          ballot = value.ballot,
          instance = value.instance,
          command = state.get.command,
          sequenceNumber = state.get.sequenceNumber,
          dependencies = state.get.dependencies.toSeq,
          status = state.get.status.toString,
          replicaIndex = index
        )))
      }
    } else {
      replica.send(ReplicaInbound().withNack(NACK(
        oldBallot = ballotMapping.getOrElse(value.instance, lowestBallot),
        instance = value.instance
      )))
    }
  }

  private def checkPrepareOkSame(neededSize: Int, replicaIndex: Int, responses: ListBuffer[PrepareOk]): (Boolean, PrepareOk, Boolean, PrepareOk) = {
    val responseMap: mutable.Map[PrepareOk, Int] = mutable.Map()
    val responseMapExclude: mutable.Map[PrepareOk, Int] = mutable.Map()

    for (response <- responses) {
      responseMap.put(response, responseMap.getOrElse(response, 0) + 1)
      if (response.instance.leaderIndex != replicaIndex) {
        responseMapExclude.put(response, responseMapExclude.getOrElse(response, 0) + 1)
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

  private def handleExplicitPrepareOk(src: Transport#Address, value: PrepareOk): Unit = {
    val prepareValues: ListBuffer[PrepareOk] = prepareResponses.getOrElse(value.instance, ListBuffer.empty)
    prepareValues.append(value)
    val quorumSize: Int = (config.n / 2) + 1
    if (prepareValues.size >= quorumSize) {
      val maxBallot: Int = prepareValues.maxBy(_.ballot.ordering).ballot.ordering
      val R: ListBuffer[PrepareOk] = prepareValues.filter(_.ballot.ordering == maxBallot)
      val sameMessage: (Boolean, PrepareOk, Boolean, PrepareOk) = checkPrepareOkSame(config.n / 2, value.instance.leaderIndex, R)
      if (R.exists(_.status.equals(ReplicaStatus.Committed.toString))) {
        val message: PrepareOk = R.filter(_.status.eq(ReplicaStatus.Committed.toString)).head
        runCommitPhase(message.command, message.sequenceNumber, message.dependencies, message.instance)
      } else if (R.exists(_.status.equals(ReplicaStatus.Accepted.toString))) {
        val message: PrepareOk = R.filter(_.status.equals(ReplicaStatus.Accepted.toString)).head
        startPaxosAcceptPhase(message.command, message.sequenceNumber, mutable.Set(message.dependencies:_*), message.instance)
      } else if (sameMessage._1) {
        startPaxosAcceptPhase(sameMessage._2.command, sameMessage._2.sequenceNumber, mutable.Set(sameMessage._2.dependencies:_*), sameMessage._2.instance)
      } else if (sameMessage._3) {
        startPhaseOne(sameMessage._4.instance, sameMessage._4.command, sameMessage._4)
      } else {
        startPhaseOne(sameMessage._4.instance, Command(null, -1, ByteString.copyFromUtf8("Noop")), sameMessage._4)
      }
    }
  }

  private def buildDependencyGraph(startInstance: Instance): DependencyGraph = {
    val graph: DependencyGraph = new DependencyGraph()
    logger.info("Command size: " + commands.size)

    def buildSubGraph(commandInstance: Instance): Unit = {
      val state: Option[State] = commands.get(commandInstance)
      if (state.nonEmpty) {
        val command: Command = state.get.command
        val commandSeqNum: Int = state.get.sequenceNumber
        val edgeList: ListBuffer[(Command, Int)] = ListBuffer()

        for (edge <- state.get.dependencies) {
          val edgeState: Option[State] = commands.get(edge)
          if (edgeState.nonEmpty) {
            edgeList.append((edgeState.get.command, edgeState.get.sequenceNumber))
          }
        }
        logger.info("Got to adding empty edge graph: " + command.toString)
        //graph.addNeighbors((command, commandSeqNum), edgeList)
        graph.addCommands((command, commandSeqNum), edgeList)
      }
    }

    val startInstanceState: Option[State] = commands.get(startInstance)
    logger.info("Started building graph")
    if (startInstanceState.nonEmpty) {
      if (startInstanceState.get.status == ReplicaStatus.Committed) {
        buildSubGraph(startInstance)
        commands.remove(startInstance)
        logger.info("Reached committed")
        for (dep <- startInstanceState.get.dependencies) {
          val depState: Option[State] = commands.get(dep)
          if (depState.nonEmpty) {
            if (depState.get.status == ReplicaStatus.Committed) {
              buildSubGraph(dep)
              commands.remove(dep)
              logger.info("Removed dep 12")
            }
          }
        }
      }
    }
    logger.info("Graph: " + graph.graph.nodes.toString)
    graph
  }

  private def executeGraph(graph: DependencyGraph): Unit = {
    //graph.executeGraph(stateMachine, executedCommands)
    graph.executeDependencyGraph(stateMachine, executedCommands)
    logger.info(graph.debug)
    //stateMachine.state.append("234")
  }
}
