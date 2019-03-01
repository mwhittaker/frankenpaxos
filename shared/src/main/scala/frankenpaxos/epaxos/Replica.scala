package frankenpaxos.epaxos

import scala.collection.mutable
import scala.scalajs.js.annotation._
import frankenpaxos._

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

case class CommandPair(commandOne: String, commandTwo: String)

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport]
) extends Actor(address, transport, logger) {
  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer

  // Verify the Paxos config and get our proposer index.
  logger.check(config.valid())
  //logger.check(config.proposerAddresses.contains(address))
  private val index: Int = config.replicaAddresses.indexOf(address)

  private val replicas: Seq[Chan[Transport, Replica[Transport]]] =
    for (replicaAddress <- config.replicaAddresses)
      yield
        chan[Replica[Transport]](
          replicaAddress,
          Replica.serializer
        )

  var nextAvailableInstance: Int = 1

  private val fastQuorumSize: Int = replicas.size - 1

  private val slowQuorumSize: Int = (replicas.size / 2) + 1

  // Tracks the set of depedencies for a given command
  //private var dependencies: mutable.Map[String, mutable.Set[Instance]] = mutable.Map()

  //private var sequenceNumbers: mutable.Map[String, Int] = mutable.Map()

  // Lookup interfering commands
  var interferenceData: mutable.Map[CommandPair, Boolean] = mutable.Map()

  object ReplicaStatus extends Enumeration {
    type ReplicaStatus = Value
    val PreAccepted = Value("PreAccepted")
    val Accepted = Value("Accepted")
    val Committed = Value("Committed")
    val Executed = Value("Executed")
  }
  import ReplicaStatus._

  case class State(command: String, sequenceNumber: Int, dependencies: mutable.Set[Instance], status: ReplicaStatus)

  // Each replica's commands private state
  var commands: mutable.Map[Instance, State] = mutable.Map()

  // The preaccept responses
  var preacceptResponses: mutable.Map[Instance, mutable.ListBuffer[PreAcceptOk]] = mutable.Map()

  // The accept ok responses
  var acceptOkResponses: mutable.Map[Instance, mutable.ListBuffer[AcceptOk]] = mutable.Map()

  var prepareResponses: mutable.Map[Instance, mutable.ListBuffer[PrepareOk]] = mutable.Map()

  private var instanceClientMapping: mutable.Map[Instance, Transport#Address] = mutable.Map()
  interferenceData.put(CommandPair("1", "2"), true)
  interferenceData.put(CommandPair("2", "1"), true)

  val stateMachine: StateMachine = new StateMachine()

  private var ballot: Int = 0

  private var ballotMapping: mutable.Map[Instance, Ballot] = mutable.Map()


  // A list of the clients awaiting a response.
  /*private val clients: mutable.Buffer[
    Chan[Transport, Client[Transport]]
  ] = mutable.Buffer()*/

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
      case Request.Nack(r) => logger.fatal("Not supported yet")
      case Request.Empty => {
        logger.fatal("Empty ReplicaInbound encountered.")
      }
    }
  }

  private def findSequenceNumber(command: String): Int = {
    var maxSequenceNum: Int = 0
    for (instance <- commands.keys) {
      val state: Option[State] = commands.get(instance)
      if (state.nonEmpty) {
        val commandPair: CommandPair = CommandPair(command, state.get.command)
        val doesConflict: Option[Boolean] = interferenceData.get(commandPair)
        if (doesConflict.nonEmpty && doesConflict.get) {
          maxSequenceNum = Math.max(maxSequenceNum, state.get.sequenceNumber)
        }
      }
      /*if (instance.command.nonEmpty) {
        val commandPair: CommandPair = CommandPair(instance.command.get, command)
        val doesConflict: Option[Boolean] = interferenceData.get(commandPair)
        if (doesConflict.nonEmpty) {
          val sequenceNum: Option[Int] = sequenceNumbers.get(instance.command.get)
          if (sequenceNum.nonEmpty) {
            maxSequenceNum = Math.max(maxSequenceNum, sequenceNum.get)
          }
        }
      }*/
    }

    maxSequenceNum + 1
  }

  private def findDependencies(command: String): mutable.Set[Instance] = {
    val deps: mutable.Set[Instance] = mutable.Set()
    for (instance <- commands.keys) {
      val state: Option[State] = commands.get(instance)
      if (state.nonEmpty) {
        val commandPair: CommandPair = CommandPair(command, state.get.command)
        val doesConflict: Option[Boolean] = interferenceData.get(commandPair)
        if (doesConflict.nonEmpty && doesConflict.get) {
          deps.add(instance)
        }
      }

      /*val instanceCommand: Option[String] = instance.command
      if (instanceCommand.nonEmpty) {
        val commandPair: CommandPair = CommandPair(instanceCommand.get, command)
        val doesConflict: Option[Boolean] = interferenceData.get(commandPair)
        if (doesConflict.nonEmpty && doesConflict.get) {
          deps.add(instance)
        }
      }*/
    }
    deps
  }

  private def handleClientRequest(address: Transport#Address, value: Request): Unit = {
    //println("Received command: " + value.command)
    val instance: Instance = Instance(index, nextAvailableInstance)
    //instances.put(nextAvailableInstance, value.command)
    instanceClientMapping.put(instance, address)
    nextAvailableInstance += 1
    //sequenceNumbers.put(value.command, findSequenceNumber(value.command))
    //dependencies.put(value.command, findDependencies(value.command))
    val seqCommand: Int = findSequenceNumber(value.command)
    val seqDeps: mutable.Set[Instance] = findDependencies(value.command)
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
          avoid = false
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
          avoid = false
        )))
  }

  private def startPhaseOne(instance: Instance, newCommand: String, value: PrepareOk): Unit = {
    val seqCommand: Int = findSequenceNumber(newCommand)
    val seqDeps: mutable.Set[Instance] = findDependencies(newCommand)
    commands.put(instance, State(newCommand, seqCommand, seqDeps, ReplicaStatus.PreAccepted))

    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != fastQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(ReplicaInbound().withPreAccept(PreAccept(
          command = newCommand,
          sequenceNumber = seqCommand,
          dependencies = seqDeps.toSeq,
          commandInstance = instance,
          avoid = true
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
          avoid = true
        )))
  }

  private def handlePreAccept(address: Transport#Address, value: PreAccept): Unit = {
    val maxSequence: Int = findSequenceNumber(value.command)
    //sequenceNumbers.put(value.command, Math.max(maxSequence + 1, sequenceNumbers.getOrElse(value.command, 1)))
    var seqNum: Int = value.sequenceNumber
    seqNum = Math.max(seqNum, maxSequence + 1)

    val depsLocal: mutable.Set[Instance] = findDependencies(value.command)
    var deps: mutable.Set[Instance] = mutable.Set(value.dependencies:_*)
    deps = deps.union(depsLocal)
    //dependencies.put(value.command, mutable.Set(value.dependencies:_*).union(depsLocal))

    commands.put(value.commandInstance, State(value.command, seqNum, deps, ReplicaStatus.PreAccepted))
    val leader = replicas(value.commandInstance.leaderIndex)
    leader.send(ReplicaInbound().withPreAcceptOk(
      PreAcceptOk(
        value.command,
        seqNum,
        deps.toSeq,
        value.commandInstance,
        value.avoid
      )
    ))
  }

  private def checkSameSequenceNumbers(preAcceptResponses: mutable.ListBuffer[PreAcceptOk], numSameNeeded: Int, total: Int): Tuple2[Boolean, Option[Int]] = {
    val sequenceNumCount: mutable.Map[Int, Int] = mutable.Map()
    for (preacceptOk <- preAcceptResponses) {
      sequenceNumCount.put(preacceptOk.sequenceNumber, sequenceNumCount.getOrElse(preacceptOk.sequenceNumber, 0) + 1)
    }

    for (key <- sequenceNumCount.keys) {
      if (sequenceNumCount.getOrElse(key, 0) >= numSameNeeded) {
        return Tuple2[Boolean, Option[Int]](true, Some(key))
      }
    }

    Tuple2[Boolean, Option[Int]](false, None)
  }

  private def checkSameDependencies(preAcceptResponses: mutable.ListBuffer[PreAcceptOk], numSameNeeded: Int, total: Int): Tuple2[Boolean, Option[Seq[Instance]]] = {
    val dependenciesNumCount: mutable.Map[Seq[Instance], Int] = mutable.Map()
    for (preacceptOk <- preAcceptResponses) {
      dependenciesNumCount.put(preacceptOk.dependencies, dependenciesNumCount.getOrElse(preacceptOk.dependencies, 0) + 1)
    }

    for (key <- dependenciesNumCount.keys) {
      if (dependenciesNumCount.getOrElse(key, 0) >= numSameNeeded) {
        return Tuple2[Boolean, Option[Seq[Instance]]](true, Some(key))
      }
    }

    Tuple2[Boolean, Option[Seq[Instance]]](false, None)
  }

  private def handlePreAcceptOk(address: Transport#Address, value: PreAcceptOk): Unit = {
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
        val seqTuple: Tuple2[Boolean, Option[Int]] = checkSameSequenceNumbers(
          preacceptResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty), N - 2, N)
        val depsTuple: Tuple2[Boolean, Option[Seq[Instance]]] = checkSameDependencies(
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
          if (value.dependencies.size > 0) {
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
  }

  private def startPaxosAcceptPhase(command: String, sequenceNum: Int, deps: mutable.Set[Instance], commandInstance: Instance): Unit = {
    commands.put(commandInstance, State(command, sequenceNum, deps, ReplicaStatus.Accepted))
    /*dependencies.put(command, deps)
    sequenceNumbers.put(command, sequenceNum)*/
    var count: Int = 0
    var setIndex: Int = 0

    while (setIndex < replicas.size && count != slowQuorumSize - 1) {
      if (setIndex != index) {
        replicas(setIndex).send(ReplicaInbound().withAccept(Accept(
          command,
          sequenceNum,
          deps.toSeq,
          commandInstance
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
      commandInstance
    )))

    /*
    for (replica <- replicas) {
      replica.send(ReplicaInbound().withAccept(Accept(
        command,
        sequenceNum,
        deps.toSeq,
        commandInstance
      )))
    }*/
  }

  private def handlePaxosAccept(src: Transport#Address, value: Accept): Unit = {
    commands.put(value.commandInstance, State(value.command, value.sequenceNumber, mutable.Set(value.dependencies:_*), ReplicaStatus.Accepted))
    val leader = replicas(value.commandInstance.leaderIndex)
    leader.send(ReplicaInbound().withAcceptOk(AcceptOk(
      command = value.command,
      commandInstance = value.commandInstance
    )))
  }

  private def handlePaxosAcceptOk(src: Transport#Address, value: AcceptOk): Unit = {
    val responseSet: mutable.ListBuffer[AcceptOk] = acceptOkResponses.getOrElse(value.commandInstance, mutable.ListBuffer.empty)
    responseSet.append(value)
    acceptOkResponses.put(value.commandInstance, responseSet)

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
  }

  private def runCommitPhase(command: String, seqNum: Int, deps: Seq[Instance], instance: Instance): Unit = {
    //println("Recevied Commit message")
    commands.put(instance, State(command, seqNum, mutable.Set(deps:_*), ReplicaStatus.Committed))
    for (replica <- replicas) {
      replica.send(ReplicaInbound().withCommit(Commit(
        command,
        seqNum,
        deps,
        instance
      )))
    }
  }

  private def handleCommit(src: Transport#Address, value: Commit): Unit = {
    commands.put(value.commandInstance, State(value.command, value.sequenceNumber, mutable.Set(value.dependencies:_*), ReplicaStatus.Committed))
    //executeGraph(buildDependencyGraph(value.commandInstance))
  }

  private def explicitPrepare(instance: Instance, oldBallot: Ballot): Unit = {
    val newBallot: Ballot = Ballot(oldBallot.ordering + 1, index)
    for (replica <- replicas) {
      replica.send(ReplicaInbound().withPrepareRequest(Prepare(
        ballot = newBallot,
        instance
      )))
    }
  }

  private def handleExplicitPrepare(src: Transport#Address, value: Prepare): Unit = {
    val replica = chan[Replica[Transport]](src, Replica.serializer)
    if (ballotMapping.get(value.instance).nonEmpty) {
      if (value.ballot.ordering > ballotMapping.get(value.instance).get.ordering) {
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
          oldBallot = ballotMapping.get(value.instance).get,
          instance = value.instance
        )))
      }
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
        startPhaseOne(sameMessage._4.instance, "no-op", sameMessage._4)
      }
    }
  }

  private def buildDependencyGraph(startInstance: Instance): DependencyGraph = {
    val graph: DependencyGraph = new DependencyGraph()

    def buildSubGraph(commandInstance: Instance): Unit = {
      val state: Option[State] = commands.get(commandInstance)
      if (state.nonEmpty) {
        val command: String = state.get.command
        val commandSeqNum: Int = state.get.sequenceNumber
        val edgeList: ListBuffer[(String, Int)] = ListBuffer()

        for (edge <- state.get.dependencies) {
          val edgeState: Option[State] = commands.get(edge)
          if (edgeState.nonEmpty) {
            edgeList.append((edgeState.get.command, edgeState.get.sequenceNumber))
          }
        }

        graph.addNeighbors((command, commandSeqNum), edgeList)
      }
    }

    val startInstanceState: Option[State] = commands.get(startInstance)
    if (startInstanceState.nonEmpty) {
      if (startInstanceState.get.status == ReplicaStatus.Committed) {
        buildSubGraph(startInstance)
        for (dep <- startInstanceState.get.dependencies) {
          val depState: Option[State] = commands.get(dep)
          if (depState.nonEmpty) {
            if (depState.get.status == ReplicaStatus.Committed) {
              buildSubGraph(dep)
            }
          }
        }
      }
    }
    graph
  }

  private def executeGraph(graph: DependencyGraph): Unit = {
    graph.executeCommands(stateMachine)
  }
}
