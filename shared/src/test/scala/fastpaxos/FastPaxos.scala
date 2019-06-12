package frankenpaxos.fastpaxos

import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class FastPaxos(val f: Int) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = f + 1
  val numLeaders = f + 1
  val numAcceptors = 2 * f + 1

  // Configuration.
  val config = Config[FakeTransport](
    f = f,
    leaderAddresses = for (i <- 1 to numLeaders)
      yield FakeTransportAddress(s"Leader $i"),
    acceptorAddresses = for (i <- 1 to numAcceptors)
      yield FakeTransportAddress(s"Acceptor $i")
  )

  // Clients.
  val clients = for (i <- 1 to numClients)
    yield
      new Client[FakeTransport](FakeTransportAddress(s"Client $i"),
                                transport,
                                logger,
                                config)

  // Leaders.
  val leaders = for (i <- 1 to numLeaders)
    yield
      new Leader[FakeTransport](FakeTransportAddress(s"Leader $i"),
                                transport,
                                logger,
                                config)

  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors)
    yield
      new Acceptor[FakeTransport](FakeTransportAddress(s"Acceptor $i"),
                                  transport,
                                  logger,
                                  config)
}

object SimulatedFastPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, value: String) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedFastPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedFastPaxos._

  override type System = FastPaxos
  // The set of chosen values.
  override type State = Set[String]
  override type Command = SimulatedFastPaxos.Command

  override def newSystem(): System = new FastPaxos(f)

  override def getState(fastPaxos: System): State = {
    // TODO(mwhittaker): Add values chosen by acceptors.

    // First, we look at any chosen values that the clients and leaders have
    // learned.
    val clientChosen = fastPaxos.clients.flatMap(_.chosenValue).to[Set]
    val leaderChosen = fastPaxos.leaders.flatMap(_.chosenValue).to[Set]
    clientChosen ++ leaderChosen
  }

  override def generateCommand(fastPaxos: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      fastPaxos.numClients -> {
        for {
          clientId <- Gen.choose(0, fastPaxos.numClients - 1)
          value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString(""))
        } yield Propose(clientId, value)
      }
    )
    FakeTransport
      .generateCommandWithFrequency(fastPaxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    if (state.size > 1) {
      SimulatedSystem.InvariantViolated(
        s"Multiple values have been chosen: $state"
      )
    } else {
      SimulatedSystem.InvariantHolds
    }
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    if (oldState.subsetOf(newState)) {
      SimulatedSystem.InvariantHolds
    } else {
      SimulatedSystem.InvariantViolated(
        s"Different values have been chosen: $oldState and then $newState."
      )
    }
  }

  override def runCommand(
      fastPaxos: System,
      command: Command
  ): System = {
    command match {
      case Propose(clientId, value) =>
        fastPaxos.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(fastPaxos.transport, command)
    }
    fastPaxos
  }

  def commandToString(command: Command): String = {
    val fastPaxos = new FastPaxos(f)
    command match {
      case Propose(clientIndex, value) =>
        val clientAddress = fastPaxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $value)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = fastPaxos.transport.actors(msg.dst)
        val s = dstActor.serializer.toPrettyString(
          dstActor.serializer.fromBytes(msg.bytes.to[Array])
        )
        s"DeliverMessage(src=${msg.src.address}, dst=${msg.dst.address})\n$s"

      case TransportCommand(FakeTransport.TriggerTimer(address, name, id)) =>
        s"TriggerTimer(${address.address}:$name $id)"
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
