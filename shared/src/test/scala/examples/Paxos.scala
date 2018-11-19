package zeno.examples

import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable
import zeno.FakeLogger
import zeno.FakeTransport
import zeno.FakeTransportAddress
import zeno.SimulatedSystem

class Paxos(val f: Int) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = f + 1
  val numProposers = f + 1
  val numAcceptors = 2 * f + 1

  // Configuration.
  val config = PaxosConfig[FakeTransport](
    f = f,
    proposerAddresses = for (i <- 1 to numProposers)
      yield FakeTransportAddress(s"Proposer $i"),
    acceptorAddresses = for (i <- 1 to numAcceptors)
      yield FakeTransportAddress(s"Acceptor $i")
  )

  // Clients.
  val clients = for (i <- 1 to numClients)
    yield
      new PaxosClientActor[FakeTransport](
        FakeTransportAddress(s"Client $i"),
        transport,
        logger,
        config
      )

  // Proposers.
  val proposers = for (i <- 1 to numProposers)
    yield
      new PaxosProposerActor[FakeTransport](
        FakeTransportAddress(s"Proposer $i"),
        transport,
        logger,
        config
      )

  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors)
    yield
      new PaxosAcceptorActor[FakeTransport](
        FakeTransportAddress(s"Acceptor $i"),
        transport,
        logger,
        config
      )

  // The current chosen values.
  def computeCurrentChosenValues(): Set[String] = {
    // First, we look at any chosen values that the clients and proposers have
    // learned.
    val clientChosen = clients.flatMap(_.chosenValue).to[Set]
    val proposerChosen = proposers.flatMap(_.chosenValue).to[Set]

    // Next, we compute any value chosen by the acceptors. A value is
    // considered chosen if it has a majority of votes in the same round.
    val votes: Seq[(Int, String)] = acceptors.flatMap(acceptor => {
      acceptor.voteValue.map((acceptor.voteRound, _))
    })
    val acceptorChosen: Set[String] =
      votes
        .filter(round_and_value => {
          votes.count(_ == round_and_value) >= f + 1
        })
        .map(_._2)
        .to[Set]

    clientChosen ++ proposerChosen ++ acceptorChosen
  }

  // All chosen values.
  var chosenValues = Set[String]()
}

sealed trait PaxosCommand
case class Propose(clientIndex: Int, value: String) extends PaxosCommand
case class TransportCommand(command: FakeTransport.Command) extends PaxosCommand

class SimulatedPaxos(val f: Int) extends SimulatedSystem[SimulatedPaxos] {
  override type System = Paxos
  override type State = Set[String]
  override type Command = PaxosCommand

  override def newSystem(): SimulatedPaxos#System = {
    new Paxos(f)
  }

  override def getState(system: SimulatedPaxos#System): SimulatedPaxos#State = {
    system.chosenValues
  }

  override def invariantHolds(
      newState: SimulatedPaxos#State,
      oldState: Option[SimulatedPaxos#State]
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
      system: SimulatedPaxos#System
  ): Option[SimulatedPaxos#Command] = {
    var subgens = mutable.Buffer[(Int, Gen[SimulatedPaxos#Command])]()

    subgens += (
      (
        system.numClients,
        for (clientId <- Gen.choose(0, system.numClients - 1);
             value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString("")))
          yield Propose(clientId, value)
      )
    )

    if ((system.transport.messages.size +
          system.transport.runningTimers().size) > 0) {
      subgens += (
        (
          system.transport.messages.size +
            system.transport.runningTimers().size,
          FakeTransport
            .generateCommand(system.transport)
            .map(TransportCommand(_))
        )
      )
    }

    val gen: Gen[SimulatedPaxos#Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(
      system: SimulatedPaxos#System,
      command: SimulatedPaxos#Command
  ): Unit = {
    command match {
      case Propose(clientId, value) =>
        system.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(system.transport, command)
    }
    system.chosenValues = system.chosenValues ++ system
      .computeCurrentChosenValues()
  }
}
