package frankenpaxos.paxos

import frankenpaxos.simulator._
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

sealed trait PaxosCommand
case class Propose(clientIndex: Int, value: String) extends PaxosCommand
case class TransportCommand(command: FakeTransportCommand) extends PaxosCommand

class SimulatedPaxos(val f: Int) extends SimulatedSystem {
  override type System = (Paxos, Set[String])
  override type State = Set[String]
  override type Command = PaxosCommand

  def chosenValues(paxos: Paxos): Set[String] = {
    // First, we look at any chosen values that the clients and leaders have
    // learned.
    val clientChosen = paxos.clients.flatMap(_.chosenValue).to[Set]
    val leaderChosen = paxos.leaders.flatMap(_.chosenValue).to[Set]

    // Next, we compute any value chosen by the acceptors. A value is
    // considered chosen if it has a majority of votes in the same round.
    val votes: Seq[(Int, String)] = paxos.acceptors.flatMap(acceptor => {
      acceptor.voteValue.map((acceptor.voteRound, _))
    })
    val acceptorChosen: Set[String] =
      votes
        .filter(round_and_value => {
          votes.count(_ == round_and_value) >= f + 1
        })
        .map(_._2)
        .to[Set]

    clientChosen ++ leaderChosen ++ acceptorChosen
  }

  override def newSystem(): System = {
    (new Paxos(f), Set())
  }

  override def getState(system: System): State = {
    system._2
  }

  override def invariantHolds(
      newState: State,
      oldState: Option[State]
  ): Option[String] = {
    if (newState.size > 1) {
      return Some(
        s"Multiple values have been chosen: $newState (previously $oldState)."
      )
    }

    if (oldState.isDefined && !oldState.get.subsetOf(newState)) {
      return Some(
        s"Different values have been chosen: ${oldState.get} and " +
          s"then $newState."
      )
    }

    None
  }

  override def generateCommand(
      system: System
  ): Option[Command] = {
    val (paxos, _) = system

    var subgens = mutable.Buffer[(Int, Gen[Command])]()
    subgens += (
      (
        paxos.numClients,
        for (clientId <- Gen.choose(0, paxos.numClients - 1);
             value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString("")))
          yield Propose(clientId, value)
      )
    )

    if ((paxos.transport.messages.size +
          paxos.transport.runningTimers().size) > 0) {
      subgens += (
        (
          paxos.transport.messages.size +
            paxos.transport.runningTimers().size,
          FakeTransport
            .generateCommand(paxos.transport)
            .map(TransportCommand(_))
        )
      )
    }

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(
      system: System,
      command: Command
  ): System = {
    val (paxos, allChosenValues) = system
    command match {
      case Propose(clientId, value) =>
        paxos.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(paxos.transport, command)
    }
    (paxos, allChosenValues ++ chosenValues(paxos))
  }

  def commandToString(command: Command): String = {
    val paxos = new Paxos(f)
    command match {
      case Propose(clientIndex, value) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $value)"

      case TransportCommand(DeliverMessage(msg)) =>
        val dstActor = paxos.transport.actors(msg.dst)
        val s = dstActor.serializer.toPrettyString(
          dstActor.serializer.fromBytes(msg.bytes.to[Array])
        )
        s"DeliverMessage(src=${msg.src.address}, dst=${msg.dst.address})\n$s"

      case TransportCommand(TriggerTimer((address, name))) =>
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
