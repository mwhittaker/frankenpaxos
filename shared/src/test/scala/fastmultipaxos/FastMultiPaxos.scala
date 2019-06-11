package frankenpaxos.fastmultipaxos

import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.AppendLog
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class FastMultiPaxos(val f: Int) {
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
    leaderElectionAddresses = for (i <- 1 to numLeaders)
      yield FakeTransportAddress(s"Leader Election $i"),
    leaderHeartbeatAddresses = for (i <- 1 to numLeaders)
      yield FakeTransportAddress(s"Leader Heartbeat $i"),
    acceptorAddresses = for (i <- 1 to numAcceptors)
      yield FakeTransportAddress(s"Acceptor $i"),
    acceptorHeartbeatAddresses = for (i <- 1 to numAcceptors)
      yield FakeTransportAddress(s"Acceptor Heartbeat $i"),
    roundSystem = new RoundSystem.MixedRoundRobin(numLeaders)
  )

  // Clients.
  val clients = for (i <- 1 to numClients)
    yield
      new Client[FakeTransport](FakeTransportAddress(s"Client $i"),
                                transport,
                                logger,
                                config,
                                ClientOptions.default,
                                new ClientMetrics(FakeCollectors))

  // Leaders.
  val leaders = for (i <- 1 to numLeaders)
    yield
      new Leader[FakeTransport](FakeTransportAddress(s"Leader $i"),
                                transport,
                                logger,
                                config,
                                new AppendLog(),
                                LeaderOptions.default,
                                new LeaderMetrics(FakeCollectors))

  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors)
    yield
      new Acceptor[FakeTransport](FakeTransportAddress(s"Acceptor $i"),
                                  transport,
                                  logger,
                                  config,
                                  AcceptorOptions.default,
                                  new AcceptorMetrics(FakeCollectors))
}

object SimulatedFastMultiPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, clientPseudonym: Int, value: String)
      extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedFastMultiPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedFastMultiPaxos._

  override type System = FastMultiPaxos
  // TODO(mwhittaker): Implement.
  override type State = Unit
  override type Command = SimulatedFastMultiPaxos.Command

  override def newSystem(): System = new FastMultiPaxos(f)

  override def getState(system: System): State = {
    // TODO(mwhittaker): Implement.
  }

  override def generateCommand(fastMultiPaxos: System): Option[Command] = {
    var subgens = mutable.Buffer[(Int, Gen[Command])]()
    subgens +=
      fastMultiPaxos.numClients -> {
        for {
          clientId <- Gen.choose(0, fastMultiPaxos.numClients - 1);
          clientPseudonym <- Gen.choose(0, 1);
          value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString(""))
        } yield Propose(clientId, clientPseudonym, value)
      }

    val numTransportItems = fastMultiPaxos.transport.messages.size +
      fastMultiPaxos.transport.runningTimers().size
    if (numTransportItems > 0) {
      subgens +=
        numTransportItems ->
          FakeTransport
            .generateCommand(fastMultiPaxos.transport)
            .map(TransportCommand(_))
    }

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(fastMultiPaxos: System, command: Command): System = {
    command match {
      case Propose(clientId, clientPseudonym, value) =>
        fastMultiPaxos.clients(clientId).propose(clientPseudonym, value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(fastMultiPaxos.transport, command)
    }
    fastMultiPaxos
  }

  // TODO(mwhittaker): Implement invariants.

  def commandToString(command: Command): String = {
    val fastMultiPaxos = new FastMultiPaxos(f)
    command match {
      case Propose(clientIndex, clientPseudonym, value) =>
        val clientAddress = fastMultiPaxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $clientPseudonym, $value)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = fastMultiPaxos.transport.actors(msg.dst)
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
