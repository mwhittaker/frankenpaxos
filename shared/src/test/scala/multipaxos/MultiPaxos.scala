package frankenpaxos.multipaxos

import frankenpaxos.simulator._
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class MultiPaxos(val f: Int) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = f + 1
  val numReplicas = f + 1
  val numLeaders = 1
  val numAcceptors = 2 * f + 1

  // Configuration.
  val config = Config[FakeTransport](
    f = f,
    replicaAddresses = for (i <- 1 to numReplicas)
      yield FakeTransportAddress(s"Replica $i"),
    acceptorAddresses = for (i <- 1 to numAcceptors)
      yield FakeTransportAddress(s"Acceptor $i"),
    leaderAddresses = for (i <- 1 to numLeaders)
      yield FakeTransportAddress(s"Leader $i")
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

  // Replicas
  val replicas = for (i <- 1 to numReplicas)
    yield
      new Replica[FakeTransport](
        FakeTransportAddress(s"Replica $i"),
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

  // Leaders
  val leaders = for (i <- 1 to numLeaders)
    yield
      new Leader[FakeTransport](
        FakeTransportAddress(s"Leader $i"),
        transport,
        logger,
        config
      )
}

sealed trait MultiPaxosCommand
case class Propose(clientIndex: Int, value: String) extends MultiPaxosCommand
case class TransportCommand(command: FakeTransportCommand)
    extends MultiPaxosCommand

class SimulatedMultiPaxos(val f: Int) extends SimulatedSystem {
  override type System = (MultiPaxos, Set[String])
  override type State = Set[String]
  override type Command = MultiPaxosCommand

  def chosenValues(multiPaxos: MultiPaxos): Set[String] = {
    // First, we look at any chosen values that the clients and proposers have
    // learned.
    //val clientChosen = paxos.clients.flatMap(_.chosenValue).to[Set]
    //val proposerChosen = paxos.proposers.flatMap(_.chosenValue).to[Set]

    // Next, we compute any value chosen by the acceptors. A value is
    // considered chosen if it has a majority of votes in the same round.
    //val votes: Seq[(Int, String)] = paxos.acceptors.flatMap(acceptor => {
    //  acceptor.voteValue.map((acceptor.voteRound, _))
    //})
    //val acceptorChosen: Set[String] =
    //  votes
    //    .filter(round_and_value => {
    //      votes.count(_ == round_and_value) >= f + 1
    //    })
    //    .map(_._2)
    //    .to[Set]

    //clientChosen ++ proposerChosen ++ acceptorChosen
    var slotToTest: Int = 3
    var commands: Set[String] = Set()
    for (replica <- multiPaxos.replicas) {
      var proposals: Set[ClientProposal] = replica.decisions
      var pairs: String = ""
      for (clientProposal <- proposals) {
        if (clientProposal.slot == slotToTest) {
          commands += clientProposal.command
        }
        //pairs = pairs + "(" + clientProposal.slot + ", " + clientProposal.command + ")\n"

      }
      //commands += pairs
    }
    commands
  }

  override def newSystem(): System = {
    (new MultiPaxos(f), Set())
  }

  override def getState(
      system: System
  ): State = {
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
    val (multiPaxos, _) = system

    var subgens = mutable.Buffer[(Int, Gen[Command])]()
    subgens += (
      (
        multiPaxos.numClients,
        for (clientId <- Gen.choose(0, multiPaxos.numClients - 1);
             value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString("")))
          yield Propose(clientId, value)
      )
    )

    if ((multiPaxos.transport.messages.size +
          multiPaxos.transport.runningTimers().size) > 0) {
      subgens += (
        (
          multiPaxos.transport.messages.size +
            multiPaxos.transport.runningTimers().size,
          FakeTransport
            .generateCommand(multiPaxos.transport)
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
    val (multiPaxos, allChosenValues) = system
    command match {
      case Propose(clientId, value) =>
        //println("Propose to client: " + clientId + " with value " + value)
        multiPaxos.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(multiPaxos.transport, command)
    }
    (multiPaxos, allChosenValues ++ chosenValues(multiPaxos))
  }
}
