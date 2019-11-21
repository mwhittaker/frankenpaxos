package frankenpaxos.matchmakerpaxos

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

class MatchmakerPaxos(val f: Int, seed: Long) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2
  val numLeaders = f + 1
  val numMatchmakers = 2 * f + 1
  val numAcceptors = 2 * (f + 1) + 1

  val config = Config[FakeTransport](
    f = f,
    leaderAddresses =
      (1 to numLeaders).map(i => FakeTransportAddress(s"Leader $i")),
    matchmakerAddresses =
      (1 to numMatchmakers).map(i => FakeTransportAddress(s"Matchmaker $i")),
    acceptorAddresses =
      (1 to numAcceptors).map(i => FakeTransportAddress(s"Acceptor $i"))
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
      options = LeaderOptions.default,
      metrics = new LeaderMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Matchmakers.
  val matchmakers = for (address <- config.matchmakerAddresses) yield {
    new Matchmaker[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = MatchmakerOptions.default,
      metrics = new MatchmakerMetrics(FakeCollectors)
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
}

object SimulatedMatchmakerPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, value: String) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedMatchmakerPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedMatchmakerPaxos._

  override type System = MatchmakerPaxos
  // We record the set of all values that people learn are chosen.
  override type State = Set[String]
  override type Command = SimulatedMatchmakerPaxos.Command

  // True if some value has been chosen in some execution of the system. Seeing
  // whether any value has been chosen is a very coarse way of testing
  // liveness. If no value is every chosen, then clearly something is wrong.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System =
    new MatchmakerPaxos(f, seed)

  override def getState(paxos: System): State = {
    val chosens = mutable.Set[String]()

    for (leader <- paxos.leaders) {
      leader.state match {
        case leader.Chosen(chosen) => chosens += chosen
        case _                     =>
      }
    }

    for (client <- paxos.clients) {
      client.state match {
        case client.Chosen(chosen) => chosens += chosen
        case _                     =>
      }
    }

    if (!chosens.isEmpty) {
      valueChosen = true
    }

    chosens.toSet
  }

  override def generateCommand(paxos: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      paxos.numClients -> {
        for {
          clientId <- Gen.choose(0, paxos.numClients - 1)
          request <- Gen.alphaLowerStr
        } yield Propose(clientId, request)
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
      case Propose(clientId, request) =>
        paxos.clients(clientId).propose(request)
      case TransportCommand(command) =>
        FakeTransport.runCommand(paxos.transport, command)
    }
    paxos
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    if (state.size > 1) {
      SimulatedSystem.InvariantViolated(s"Multiple values were chosen: $state.")
    } else {
      SimulatedSystem.InvariantHolds
    }
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    if (!oldState.subsetOf(newState)) {
      SimulatedSystem.InvariantViolated(
        s"Old chosen values $oldState is not a subset of the newly chosen " +
          s"values $newState."
      )
    } else {
      SimulatedSystem.InvariantHolds
    }
  }

  def commandToString(command: Command): String = {
    val paxos = newSystem(System.currentTimeMillis())
    command match {
      case Propose(clientIndex, value) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $value)"

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
