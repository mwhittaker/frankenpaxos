package frankenpaxos.fastpaxos

import frankenpaxos.simulator._
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

sealed trait FastPaxosCommand
case class Propose(clientIndex: Int, value: String) extends FastPaxosCommand
case class TransportCommand(command: FakeTransport.Command)
    extends FastPaxosCommand

class SimulatedFastPaxos(val f: Int)
    extends SimulatedSystem[SimulatedFastPaxos] {
  // A Fast Paxos instance and the set of values chosen.
  override type System = (FastPaxos, Set[String])
  // The set of values chosen.
  override type State = Set[String]
  override type Command = FastPaxosCommand

  def chosenValues(fastPaxos: FastPaxos): Set[String] = {
    // First, we look at any chosen values that the clients and leaders have
    // learned.
    val clientChosen = fastPaxos.clients.flatMap(_.chosenValue).to[Set]
    val leaderChosen = fastPaxos.leaders.flatMap(_.chosenValue).to[Set]
    clientChosen ++ leaderChosen

    // TODO(mwhittaker): Add acceptor chosen.
  }

  override def newSystem(): SimulatedFastPaxos#System = {
    (new FastPaxos(f), Set())
  }

  override def getState(
      system: SimulatedFastPaxos#System
  ): SimulatedFastPaxos#State = system._2

  override def invariantHolds(
      newState: SimulatedFastPaxos#State,
      oldState: Option[SimulatedFastPaxos#State]
  ): Option[String] = {
    if (newState.size > 1) {
      return Some(s"""Multiple values have been chosen: $newState (previously
                     |$oldState).""".stripMargin)
    }

    if (oldState.isDefined && !oldState.get.subsetOf(newState)) {
      return Some(s"""Different values have been chosen: ${oldState.get} and
                     |then $newState.""".stripMargin)
    }

    None
  }

  override def generateCommand(
      system: SimulatedFastPaxos#System
  ): Option[SimulatedFastPaxos#Command] = {
    val (fastPaxos, _) = system

    var subgens = mutable.Buffer[(Int, Gen[SimulatedFastPaxos#Command])]()
    subgens += (
      (
        fastPaxos.numClients,
        for (clientId <- Gen.choose(0, fastPaxos.numClients - 1);
             value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString("")))
          yield Propose(clientId, value)
      )
    )

    if ((fastPaxos.transport.messages.size +
          fastPaxos.transport.runningTimers().size) > 0) {
      subgens += (
        (
          fastPaxos.transport.messages.size +
            fastPaxos.transport.runningTimers().size,
          FakeTransport
            .generateCommand(fastPaxos.transport)
            .map(TransportCommand(_))
        )
      )
    }

    val gen: Gen[SimulatedFastPaxos#Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(
      system: SimulatedFastPaxos#System,
      command: SimulatedFastPaxos#Command
  ): SimulatedFastPaxos#System = {
    val (fastPaxos, allChosenValues) = system
    command match {
      case Propose(clientId, value) =>
        fastPaxos.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(fastPaxos.transport, command)
    }
    (fastPaxos, allChosenValues ++ chosenValues(fastPaxos))
  }
}
