package frankenpaxos.mencius

import frankenpaxos.Util
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.AppendLog
import frankenpaxos.util
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class Mencius(val f: Int, batched: Boolean, seed: Long) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2
  val numBatchers = if (batched) {
    f + 1
  } else {
    0
  }
  val numLeaderGroups = 2
  val numLeadersPerGroup = f + 1
  val numProxyLeaders = f + 1
  val numAcceptorGroupsPerLeaderGroup = 2
  val numAcceptorsPerGroup = 2 * f + 1
  val numReplicas = f + 1
  val numProxyReplicas = f + 1

  val config = Config[FakeTransport](
    f = f,
    batcherAddresses =
      (1 to numBatchers).map(i => FakeTransportAddress(s"Batcher $i")),
    leaderAddresses = for (i <- 1 to numLeaderGroups) yield {
      for (j <- 1 to numLeadersPerGroup)
        yield FakeTransportAddress(s"Leader $i.$j")

    },
    leaderElectionAddresses = for (i <- 1 to numLeaderGroups) yield {
      for (j <- 1 to numLeadersPerGroup)
        yield FakeTransportAddress(s"LeaderElection $i.$j")
    },
    proxyLeaderAddresses =
      (1 to numProxyLeaders).map(i => FakeTransportAddress(s"ProxyLeader $i")),
    acceptorAddresses = for (i <- 1 to numLeaderGroups) yield {
      for (j <- 1 to numAcceptorGroupsPerLeaderGroup) yield {
        for (k <- 1 to numAcceptorsPerGroup)
          yield FakeTransportAddress(s"Acceptor $i.$j.$k")
      }
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
      options = BatcherOptions.default.copy(
        batchSize = 2
      ),
      metrics = new BatcherMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Leaders.
  val leaders = for (group <- config.leaderAddresses; address <- group) yield {
    new Leader[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = LeaderOptions.default.copy(
        sendHighWatermarkEveryN = 2,
        sendNoopRangeIfLaggingBy = 3
      ),
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
      metrics = new ProxyLeaderMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Acceptors.
  val acceptors = for {
    groups <- config.acceptorAddresses
    group <- groups
    address <- group
  } yield {
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
      stateMachine = new AppendLog(),
      config = config,
      options = ReplicaOptions.default.copy(
        logGrowSize = 5,
        unsafeDontUseClientTable = false,
        sendChosenWatermarkEveryNEntries = 5,
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

object SimulatedMencius {
  sealed trait Command
  case class Propose(
      clientIndex: Int,
      clientPseudonym: Int,
      value: String
  ) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedMencius(val f: Int, batched: Boolean) extends SimulatedSystem {
  import SimulatedMencius._

  override type System = Mencius
  // For every replica, we record the prefix of the log that has been executed.
  override type State = mutable.Buffer[Seq[CommandBatchOrNoop]]
  override type Command = SimulatedMencius.Command

  // True if some value has been chosen in some execution of the system. Seeing
  // whether any value has been chosen is a very coarse way of testing
  // liveness. If no value is every chosen, then clearly something is wrong.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System = new Mencius(f, batched, seed)

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
      // Propose.
      paxos.numClients -> {
        for {
          clientId <- Gen.choose(0, paxos.numClients - 1)
          request <- Gen.alphaLowerStr
        } yield Propose(clientId, clientPseudonym = 0, request)
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
      case Propose(clientId, clientPseudonym, request) =>
        paxos.clients(clientId).propose(clientPseudonym, request)
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
      case Propose(clientIndex, clientPseudonym, value) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $clientPseudonym, $value)"

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
