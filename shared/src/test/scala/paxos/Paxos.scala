package frankenpaxos.paxos

import frankenpaxos.Util
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class Paxos(val f: Int) {
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
      new Client[FakeTransport](
        FakeTransportAddress(s"Client $i"),
        transport,
        logger,
        config
      )

  // Leaders.
  val leaders = for (i <- 1 to numLeaders)
    yield
      new Leader[FakeTransport](
        FakeTransportAddress(s"Leader $i"),
        transport,
        logger,
        config
      )

  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors)
    yield
      new Acceptor[FakeTransport](
        FakeTransportAddress(s"Acceptor $i"),
        transport,
        logger,
        config
      )
}

object SimulatedPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, value: String) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedPaxos._

  override type System = Paxos
  // The set of strings that are chosen in the current system.
  override type State = Set[String]
  override type Command = SimulatedPaxos.Command

  override def newSystem(): System = new Paxos(f)

  override def getState(paxos: System): State = {
    // First, we look at any chosen values that the clients and leaders have
    // learned.
    val clientChosen = paxos.clients.flatMap(_.chosenValue).to[Set]
    val leaderChosen = paxos.leaders.flatMap(_.chosenValue).to[Set]

    // Next, we compute any value chosen by the acceptors. A value is
    // considered chosen if it has a majority of votes in the same round.
    val votes: Seq[(Int, String)] = paxos.acceptors.flatMap(
      acceptor => acceptor.voteValue.map((acceptor.voteRound, _))
    )
    val acceptorChosen = Util
      .popularItems(votes, f + 1)
      .map({ case (voteRound, voteValue) => voteValue })

    clientChosen ++ leaderChosen ++ acceptorChosen
  }

  override def generateCommand(paxos: System): Option[Command] = {
    var subgens = mutable.Buffer[(Int, Gen[Command])]()
    subgens += paxos.numClients -> {
      for {
        clientId <- Gen.choose(0, paxos.numClients - 1);
        value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString(""))
      } yield Propose(clientId, value)
    }

    val numTransportItems = paxos.transport.messages.size +
      paxos.transport.runningTimers().size
    if (numTransportItems > 0) {
      subgens +=
        numTransportItems ->
          FakeTransport
            .generateCommand(paxos.transport)
            .map(TransportCommand(_))
    }

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(
      paxos: System,
      command: Command
  ): System = {
    command match {
      case Propose(clientId, value) =>
        paxos.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(paxos.transport, command)
    }
    paxos
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

  private def commandToString(command: Command): String = {
    val paxos = new Paxos(f)
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

      case TransportCommand(FakeTransport.TriggerTimer((address, name))) =>
        s"TriggerTimer(${address.address}:$name)"
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
