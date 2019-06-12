package frankenpaxos.epaxos

import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class EPaxos(val f: Int) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = f + 1
  val numReplicas = 2 * f + 1

  // Configuration.
  val config = Config[FakeTransport](
    f = f,
    replicaAddresses = for (i <- 1 to numReplicas)
      yield FakeTransportAddress(s"Replica $i")
  )

  // Clients.
  val clients = for (i <- 1 to numClients)
    yield
      new Client[FakeTransport](FakeTransportAddress(s"Client $i"),
                                transport,
                                logger,
                                config)

  // Replicas
  val replicas = for (i <- 1 to numReplicas)
    yield
      new Replica[FakeTransport](FakeTransportAddress(s"Replica $i"),
                                 transport,
                                 logger,
                                 config,
                                 new Register(),
                                 new JgraphtDependencyGraph())
}

object SimulatedEPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, value: String) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedEPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedEPaxos._

  override type System = EPaxos
  // TODO(mwhittaker): Implement.
  override type State = Unit
  override type Command = SimulatedEPaxos.Command

  override def newSystem(): System = new EPaxos(f)

  override def getState(system: System): State = {
    // TODO(mwhittaker): Implement.
  }

  override def generateCommand(epaxos: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      epaxos.numClients -> {
        for {
          clientId <- Gen.choose(0, epaxos.numClients - 1)
          value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString(""))
        } yield Propose(clientId, value)
      }
    )
    FakeTransport
      .generateCommandWithFrequency(epaxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(epaxos: System, command: Command): System = {
    command match {
      case Propose(clientId, value) =>
        epaxos.clients(clientId).propose("TODO")
      case TransportCommand(command) =>
        FakeTransport.runCommand(epaxos.transport, command)
    }
    epaxos
  }

  // TODO(mwhittaker): Implement invariants.

  def commandToString(command: Command): String = {
    val epaxos = newSystem()
    command match {
      case Propose(clientIndex, value) =>
        val clientAddress = epaxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $value)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = epaxos.transport.actors(msg.dst)
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
