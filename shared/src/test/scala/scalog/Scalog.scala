package frankenpaxos.scalog

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

class Scalog(val f: Int, numProxyReplicas: Int, pushSize: Int, seed: Long) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2
  val numShards = f + 1
  val numServersPerShard = f + 1
  val numLeaders = f + 1
  val numAcceptors = 2 * f + 1
  val numReplicas = f + 1

  val config = Config[FakeTransport](
    f = f,
    serverAddresses = (1 to numShards).map(
      i =>
        (1 to numServersPerShard)
          .map(j => FakeTransportAddress(s"Server $i.$j"))
    ),
    aggregatorAddress = FakeTransportAddress("Aggregator"),
    leaderAddresses =
      (1 to numLeaders).map(i => FakeTransportAddress(s"Leader $i")),
    leaderElectionAddresses =
      (1 to numLeaders).map(i => FakeTransportAddress(s"LeaderElection $i")),
    acceptorAddresses =
      (1 to numAcceptors).map(i => FakeTransportAddress(s"Acceptor $i")),
    replicaAddresses = (1 to numReplicas)
      .map(i => FakeTransportAddress(s"Replica $i")),
    proxyReplicaAddresses = (1 to numProxyReplicas).map(
      i => FakeTransportAddress(s"Proxy Replica $i")
    )
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

  // Servers.
  val servers = for (shard <- config.serverAddresses; address <- shard) yield {
    new Server[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ServerOptions.default.copy(
        logGrowSize = 10,
        pushSize = pushSize
      ),
      metrics = new ServerMetrics(FakeCollectors)
    )
  }

  // Aggregators.
  val aggregator = new Aggregator[FakeTransport](
    address = config.aggregatorAddress,
    transport = transport,
    logger = new FakeLogger(),
    config = config,
    options = AggregatorOptions.default.copy(
      numShardCutsPerProposal = (f + 1) * (f + 1),
      logGrowSize = 10
    ),
    metrics = new AggregatorMetrics(FakeCollectors)
  )

  // Leaders.
  val leaders = for (address <- config.leaderAddresses) yield {
    new Leader[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = LeaderOptions.default.copy(
        logGrowSize = 10
      ),
      metrics = new LeaderMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Acceptors.
  val acceptors = for (address <- config.acceptorAddresses)
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
      stateMachine = new AppendLog(),
      config = config,
      options = ReplicaOptions.default.copy(
        logGrowSize = 10
      ),
      metrics = new ReplicaMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Proxy Replicas.
  val proxyReplicas = for (address <- config.proxyReplicaAddresses) yield {
    new ProxyReplica[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ProxyReplicaOptions.default.copy(
        batchFlush = true
      ),
      metrics = new ProxyReplicaMetrics(FakeCollectors)
    )
  }
}

object SimulatedScalog {
  sealed trait Command
  case class Write(clientIndex: Int, clientPseudonym: Int, value: String)
      extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedScalog(val f: Int, numProxyReplicas: Int, pushSize: Int)
    extends SimulatedSystem {
  import SimulatedScalog._

  override type System = Scalog
  // For every replica, we record the prefix of the log that has been executed.
  override type State = mutable.Buffer[Seq[frankenpaxos.scalog.Command]]
  override type Command = SimulatedScalog.Command

  // True if some value has been chosen in some execution of the system. Seeing
  // whether any value has been chosen is a very coarse way of testing
  // liveness. If no value is every chosen, then clearly something is wrong.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System =
    new Scalog(f, numProxyReplicas, pushSize, seed)

  override def getState(scalog: System): State = {
    val logs = mutable.Buffer[Seq[frankenpaxos.scalog.Command]]()
    for (replica <- scalog.replicas) {
      if (replica.executedWatermark > 0) {
        valueChosen = true
      }
      logs += (0 until replica.executedWatermark).map(replica.log.get(_).get)
    }

    logs
  }

  override def generateCommand(scalog: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Write.
      scalog.numClients -> {
        for {
          clientId <- Gen.choose(0, scalog.numClients - 1)
          request <- Gen.alphaLowerStr.filter(_.size > 0)
        } yield Write(clientId, clientPseudonym = 0, request)

      }
    )
    FakeTransport
      .generateCommandWithFrequency(scalog.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(scalog: System, command: Command): System = {
    command match {
      case Write(clientId, clientPseudonym, request) =>
        scalog.clients(clientId).write(clientPseudonym, request)
      case TransportCommand(command) =>
        FakeTransport.runCommand(scalog.transport, command)
    }
    scalog
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
    val scalog = newSystem(System.currentTimeMillis())
    command match {
      case Write(clientIndex, clientPseudonym, value) =>
        val clientAddress = scalog.clients(clientIndex).address.address
        s"Write($clientAddress, $clientPseudonym, $value)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = scalog.transport.actors(msg.dst)
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
