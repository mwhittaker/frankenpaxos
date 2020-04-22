package frankenpaxos.multipaxos

import frankenpaxos.Util
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.ReadableAppendLog
import frankenpaxos.util
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class MultiPaxos(val f: Int, batched: Boolean, seed: Long) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2
  val numBatchers = if (batched) {
    f + 1
  } else {
    0
  }
  val numLeaders = f + 1
  val numProxyLeaders = f + 1
  val numAcceptorGroups = 2
  val numAcceptors = 2 * f + 1
  val numReplicas = f + 1
  val numProxyReplicas = f + 1

  val config = Config[FakeTransport](
    f = f,
    batcherAddresses =
      (1 to numBatchers).map(i => FakeTransportAddress(s"Batcher $i")),
    readBatcherAddresses =
      (1 to numBatchers).map(i => FakeTransportAddress(s"ReadBatcher $i")),
    leaderAddresses =
      (1 to numLeaders).map(i => FakeTransportAddress(s"Leader $i")),
    leaderElectionAddresses =
      (1 to numLeaders).map(i => FakeTransportAddress(s"LeaderElection $i")),
    proxyLeaderAddresses =
      (1 to numProxyLeaders).map(i => FakeTransportAddress(s"ProxyLeader $i")),
    acceptorAddresses = for (g <- 0 to numAcceptorGroups)
      yield {
        (1 to numAcceptors).map(i => FakeTransportAddress(s"Acceptor $g.$i")),
      },
    replicaAddresses =
      (1 to numReplicas).map(i => FakeTransportAddress(s"Replica $i")),
    proxyReplicaAddresses = (1 to numProxyReplicas)
      .map(i => FakeTransportAddress(s"ProxyReplica $i")),
    distributionScheme = Hash
  )

  // Clients.
  val clients = for (i <- 1 to numClients) yield {
    new Client[FakeTransport](
      address = FakeTransportAddress(s"Client $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ClientOptions.default,
      metrics = new ClientMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Batchers.
  val batchers = for (address <- config.batcherAddresses) yield {
    new Batcher[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = BatcherOptions.default.copy(batchSize = 1),
      metrics = new BatcherMetrics(FakeCollectors),
      seed = seed
    )
  }

  // ReadBatchers.
  val readBatchers = for (address <- config.readBatcherAddresses) yield {
    new ReadBatcher[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ReadBatcherOptions.default.copy(
        readBatchingScheme =
          ReadBatchingScheme.Size(batchSize = 1,
                                  timeout = java.time.Duration.ofSeconds(1))
      ),
      metrics = new ReadBatcherMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Leaders.
  val leaders = for (address <- config.leaderAddresses) yield {
    new Leader[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = LeaderOptions.default,
      metrics = new LeaderMetrics(FakeCollectors),
      seed = seed
    )
  }

  // ProxyLeaders.
  val proxyLeaders = for (address <- config.proxyLeaderAddresses) yield {
    new ProxyLeader[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ProxyLeaderOptions.default,
      metrics = new ProxyLeaderMetrics(FakeCollectors)
    )
  }

  // Acceptors.
  val acceptors = for (group <- config.acceptorAddresses; address <- group)
    yield {
      new Acceptor[FakeTransport](
        address = address,
        transport = transport,
        logger = new FakeLogger(),
        config = config,
        options = AcceptorOptions.default,
        metrics = new AcceptorMetrics(FakeCollectors)
      )
    }

  // Replicas.
  val replicas = for (address <- config.replicaAddresses) yield {
    new Replica[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      stateMachine = new ReadableAppendLog(),
      config = config,
      options = ReplicaOptions.default.copy(
        logGrowSize = 10,
        unsafeDontUseClientTable = false,
        unsafeDontRecover = false
      ),
      metrics = new ReplicaMetrics(FakeCollectors),
      seed = seed
    )
  }

  // ProxyReplicas.
  val proxyReplicas = for (address <- config.proxyReplicaAddresses) yield {
    new ProxyReplica[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ProxyReplicaOptions.default,
      metrics = new ProxyReplicaMetrics(FakeCollectors)
    )
  }
}

object SimulatedMultiPaxos {
  sealed trait Command

  case class Write(
      clientIndex: Int,
      clientPseudonym: Int,
      value: String
  ) extends Command

  case class Read(
      clientIndex: Int,
      clientPseudonym: Int
  ) extends Command

  case class SequentialRead(
      clientIndex: Int,
      clientPseudonym: Int
  ) extends Command

  case class EventualRead(
      clientIndex: Int,
      clientPseudonym: Int
  ) extends Command

  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedMultiPaxos(val f: Int, batched: Boolean)
    extends SimulatedSystem {
  import SimulatedMultiPaxos._

  override type System = MultiPaxos
  // For every replica, we record the prefix of the log that has been executed.
  override type State = mutable.Buffer[Seq[CommandBatchOrNoop]]
  override type Command = SimulatedMultiPaxos.Command

  // True if some value has been chosen in some execution of the system. Seeing
  // whether any value has been chosen is a very coarse way of testing
  // liveness. If no value is every chosen, then clearly something is wrong.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System = new MultiPaxos(f, batched, seed)

  override def getState(paxos: System): State = {
    val logs = mutable.Buffer[Seq[CommandBatchOrNoop]]()
    for (replica <- paxos.replicas) {
      if (replica.executedWatermark > 0) {
        valueChosen = true
      }
      logs += (0 until replica.executedWatermark).map(replica.log.get(_).get)
    }

    logs
  }

  override def generateCommand(paxos: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Write.
      paxos.numClients * 3 -> {
        for {
          clientId <- Gen.choose(0, paxos.numClients - 1)
          request <- Gen.alphaLowerStr.filter(_.size > 0)
        } yield Write(clientId, clientPseudonym = 0, request)

      },
      // Read.
      paxos.numClients -> {
        for {
          clientId <- Gen.choose(0, paxos.numClients - 1)
        } yield Read(clientId, clientPseudonym = 0)
      },
      // Sequential Read.
      paxos.numClients -> {
        for {
          clientId <- Gen.choose(0, paxos.numClients - 1)
        } yield SequentialRead(clientId, clientPseudonym = 0)
      },
      // Eventual Read.
      paxos.numClients -> {
        for {
          clientId <- Gen.choose(0, paxos.numClients - 1)
        } yield EventualRead(clientId, clientPseudonym = 0)
      }
    )
    FakeTransport
      .generateCommandWithFrequency(paxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(paxos: System, command: Command): System = {
    command match {
      case Write(clientId, clientPseudonym, request) =>
        paxos.clients(clientId).write(clientPseudonym, request)
      case Read(clientId, clientPseudonym) =>
        paxos.clients(clientId).read(clientPseudonym, "")
      case SequentialRead(clientId, clientPseudonym) =>
        paxos.clients(clientId).sequentialRead(clientPseudonym, "")
      case EventualRead(clientId, clientPseudonym) =>
        paxos.clients(clientId).eventualRead(clientPseudonym, "")
      case TransportCommand(command) =>
        FakeTransport.runCommand(paxos.transport, command)
    }
    paxos
  }

  private def isPrefix[A](lhs: Seq[A], rhs: Seq[A]): Boolean = {
    lhs.zipWithIndex.forall({
      case (x, i) => rhs.lift(i) == Some(x)
    })
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    for (logs <- state.combinations(2)) {
      val lhs = logs(0)
      val rhs = logs(1)
      if (!isPrefix(lhs, rhs) && !isPrefix(rhs, lhs)) {
        return SimulatedSystem.InvariantViolated(
          s"Logs $lhs and $rhs are not compatible."
        )
      }
    }

    SimulatedSystem.InvariantHolds
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    for ((oldLog, newLog) <- oldState.zip(newState)) {
      if (!isPrefix(oldLog, newLog)) {
        return SimulatedSystem.InvariantViolated(
          s"Logs $oldLog is not a prefix of $newLog."
        )
      }
    }

    SimulatedSystem.InvariantHolds
  }

  def commandToString(command: Command): String = {
    val paxos = newSystem(System.currentTimeMillis())
    command match {
      case Write(clientIndex, clientPseudonym, value) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"Write($clientAddress, $clientPseudonym, $value)"

      case Read(clientIndex, clientPseudonym) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"Read($clientAddress, $clientPseudonym)"

      case SequentialRead(clientIndex, clientPseudonym) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"SequentialRead($clientAddress, $clientPseudonym)"

      case EventualRead(clientIndex, clientPseudonym) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"EventualRead($clientAddress, $clientPseudonym)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = paxos.transport.actors(msg.dst)
        val s = dstActor.serializer.toPrettyString(
          dstActor.serializer.fromBytes(msg.bytes.to[Array])
        )
        s"DeliverMessage(src=${msg.src.address}, dst=${msg.dst.address})\n$s"

      case TransportCommand(FakeTransport.TriggerTimer(address, name, id)) =>
        s"TriggerTimer(${address.address}:$name ($id))"
    }
  }

  def historyToString(history: Seq[Command]): String = {
    def indent(s: String, n: Int): String = {
      s.replaceAll("\n", "\n" + " " * n)
    }
    history.zipWithIndex
      .map({
        case (command, i) =>
          val num = "%3d".format(i)
          s"$num. ${indent(commandToString(command), 5)}"
      })
      .mkString("\n")
  }
}
