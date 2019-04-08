package frankenpaxos.fastmultipaxos

import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator._
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
                                config)

  // Leaders.
  val leaders = for (i <- 1 to numLeaders)
    yield
      new Leader[FakeTransport](FakeTransportAddress(s"Leader $i"),
                                transport,
                                logger,
                                config,
                                new AppendLog(),
                                new LeaderMetrics(FakeCollectors))

  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors)
    yield
      new Acceptor[FakeTransport](FakeTransportAddress(s"Acceptor $i"),
                                  transport,
                                  logger,
                                  config)
}

sealed trait FastMultiPaxosCommand
case class Propose(clientIndex: Int, value: String)
    extends FastMultiPaxosCommand
case class TransportCommand(command: FakeTransportCommand)
    extends FastMultiPaxosCommand

class SimulatedFastMultiPaxos(val f: Int)
    extends SimulatedSystem[SimulatedFastMultiPaxos] {
  // TODO(mwhittaker): Implement.
  override type System = (FastMultiPaxos, Unit)
  // TODO(mwhittaker): Implement.
  override type State = Unit
  override type Command = FastMultiPaxosCommand

  override def newSystem(): SimulatedFastMultiPaxos#System = {
    (new FastMultiPaxos(f), ())
  }

  override def getState(
      system: SimulatedFastMultiPaxos#System
  ): SimulatedFastMultiPaxos#State = system._2

  override def invariantHolds(
      newState: SimulatedFastMultiPaxos#State,
      oldState: Option[SimulatedFastMultiPaxos#State]
  ): Option[String] = {
    None
  }

  override def generateCommand(
      system: SimulatedFastMultiPaxos#System
  ): Option[SimulatedFastMultiPaxos#Command] = {
    val (fastMultiPaxos, _) = system

    var subgens = mutable.Buffer[(Int, Gen[SimulatedFastMultiPaxos#Command])]()
    subgens += (
      (
        fastMultiPaxos.numClients,
        for (clientId <- Gen.choose(0, fastMultiPaxos.numClients - 1);
             value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString("")))
          yield Propose(clientId, value)
      )
    )

    if ((fastMultiPaxos.transport.messages.size +
          fastMultiPaxos.transport.runningTimers().size) > 0) {
      subgens += (
        (
          fastMultiPaxos.transport.messages.size +
            fastMultiPaxos.transport.runningTimers().size,
          FakeTransport
            .generateCommand(fastMultiPaxos.transport)
            .map(TransportCommand(_))
        )
      )
    }

    val gen: Gen[SimulatedFastMultiPaxos#Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(
      system: SimulatedFastMultiPaxos#System,
      command: SimulatedFastMultiPaxos#Command
  ): SimulatedFastMultiPaxos#System = {
    val (fastMultiPaxos, allChosenValues) = system
    command match {
      case Propose(clientId, value) =>
        fastMultiPaxos.clients(clientId).propose(value)
      case TransportCommand(command) =>
        FakeTransport.runCommand(fastMultiPaxos.transport, command)
    }
    (fastMultiPaxos, ())
  }

  def commandToString(command: SimulatedFastMultiPaxos#Command): String = {
    val fastMultiPaxos = new FastMultiPaxos(f)
    command match {
      case Propose(clientIndex, value) =>
        val clientAddress = fastMultiPaxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $value)"

      case TransportCommand(DeliverMessage(msg)) =>
        val dstActor = fastMultiPaxos.transport.actors(msg.dst)
        val s = dstActor.serializer.toPrettyString(
          dstActor.serializer.fromBytes(msg.bytes.to[Array])
        )
        s"DeliverMessage(src=${msg.src.address}, dst=${msg.dst.address})\n$s"

      case TransportCommand(TriggerTimer((address, name))) =>
        s"TriggerTimer(${address.address}:$name)"
    }
  }

  def historyToString(history: Seq[SimulatedFastMultiPaxos#Command]): String = {
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
