package frankenpaxos.horizontal

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

class Horizontal(val f: Int, seed: Long) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2
  val numLeaders = f + 1
  val numAcceptors = 2 * (2 * f + 1)
  val numReplicas = f + 1

  val config = Config[FakeTransport](
    f = f,
    leaderAddresses =
      (1 to numLeaders).map(i => FakeTransportAddress(s"Leader $i")),
    leaderElectionAddresses =
      (1 to numLeaders).map(i => FakeTransportAddress(s"LeaderElection $i")),
    acceptorAddresses =
      (1 to numAcceptors).map(i => FakeTransportAddress(s"Acceptor $i")),
    replicaAddresses =
      (1 to numReplicas).map(i => FakeTransportAddress(s"Replica $i"))
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

  // Leaders.
  val leaders = for (address <- config.leaderAddresses) yield {
    new Leader[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = LeaderOptions.default.copy(alpha = 3),
      metrics = new LeaderMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Acceptors.
  val acceptors = for (address <- config.acceptorAddresses) yield {
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
      options = ReplicaOptions.default,
      metrics = new ReplicaMetrics(FakeCollectors)
    )
  }
}

object SimulatedHorizontal {
  sealed trait Command

  case class Propose(
      clientIndex: Int,
      clientPseudonym: Int,
      value: String
  ) extends Command

  case object Reconfigure extends Command

  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedHorizontal(val f: Int) extends SimulatedSystem {
  import SimulatedHorizontal._

  override type System = Horizontal
  // For every replica, we record the prefix of the log that has been executed.
  override type State = mutable.Buffer[Seq[Value]]
  override type Command = SimulatedHorizontal.Command

  // True if some value has been chosen in some execution of the system. Seeing
  // whether any value has been chosen is a very coarse way of testing
  // liveness. If no value is every chosen, then clearly something is wrong.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System = new Horizontal(f, seed)

  override def getState(horizontal: System): State = {
    val logs = mutable.Buffer[Seq[Value]]()
    for (replica <- horizontal.replicas) {
      if (replica.executedWatermark > 0) {
        valueChosen = true
      }
      logs += (0 until replica.executedWatermark).map(replica.log.get(_).get)
    }

    logs
  }

  override def generateCommand(horizontal: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      horizontal.numClients -> {
        for {
          clientId <- Gen.choose(0, horizontal.numClients - 1)
          request <- Gen.alphaLowerStr.filter(_.size > 0)
        } yield Propose(clientId, clientPseudonym = 0, request)
      },
      1 -> Gen.const(Reconfigure)
    )
    FakeTransport
      .generateCommandWithFrequency(horizontal.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(horizontal: System, command: Command): System = {
    command match {
      case Propose(clientId, clientPseudonym, request) =>
        horizontal.clients(clientId).propose(clientPseudonym, request)
      case Reconfigure =>
        horizontal.leaders.foreach(_.reconfigure())
      case TransportCommand(command) =>
        FakeTransport.runCommand(horizontal.transport, command)
    }
    horizontal
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
    val horizontal = newSystem(System.currentTimeMillis())
    command match {
      case Propose(clientIndex, clientPseudonym, value) =>
        val clientAddress = horizontal.clients(clientIndex).address.address
        s"Propose($clientAddress, $clientPseudonym, $value)"

      case Reconfigure =>
        "Reconfigure"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = horizontal.transport.actors(msg.dst)
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
