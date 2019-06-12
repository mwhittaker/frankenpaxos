package frankenpaxos.multipaxos

import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
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

object SimulatedMultiPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, value: String) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedMultiPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedMultiPaxos._

  override type System = MultiPaxos
  // TODO(mwhittaker): Implement.
  override type State = Unit
  override type Command = SimulatedMultiPaxos.Command

  override def newSystem(): System = new MultiPaxos(f)

  override def getState(system: System): State = {
    // TODO(mwhittaker): Implement.
  }

  override def generateCommand(multiPaxos: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      multiPaxos.numClients -> {
        for {
          clientId <- Gen.choose(0, multiPaxos.numClients - 1)
          value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString(""))
        } yield Propose(clientId, value)
      }
    )
    FakeTransport
      .generateCommandWithFrequency(multiPaxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(multiPaxos: System, command: Command): System = {
    command match {
      case Propose(clientId, value) =>
        multiPaxos.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(multiPaxos.transport, command)
    }
    multiPaxos
  }

  // TODO(mwhittaker): Add invariants.
}
