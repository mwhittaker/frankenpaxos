package frankenpaxos.caspaxos

import frankenpaxos.Util
import frankenpaxos.Util.MapHelpers
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.util
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

class CasPaxos(val f: Int, seed: Long) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2 * f + 1
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
  val clients = for (i <- 1 to numClients) yield {
    new Client[FakeTransport](
      address = FakeTransportAddress(s"Client $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ClientOptions.default.copy(),
      metrics = new ClientMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Leaders.
  val leaders = for (i <- 1 to numLeaders) yield {
    new Leader[FakeTransport](
      address = FakeTransportAddress(s"Leader $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = LeaderOptions.default.copy(),
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }
  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors) yield {
    new Acceptor[FakeTransport](
      address = FakeTransportAddress(s"Acceptor $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = AcceptorOptions.default.copy(),
      metrics = new AcceptorMetrics(FakeCollectors)
    )
  }

  // Futures registered from client proposals.
  val futures = mutable.Buffer[Future[Set[Int]]]()
}

object SimulatedCasPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, value: Set[Int]) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedCasPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedCasPaxos._

  override type System = CasPaxos
  // We store the set of all int sets received by clients.
  override type State = Set[Set[Int]]
  override type Command = SimulatedCasPaxos.Command

  // True if some value has been chosen in some execution of the system.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System = new CasPaxos(f, seed)

  override def getState(caspaxos: System): State = {
    val sets = caspaxos.futures
      .flatMap(future => {
        future.value match {
          case Some(scala.util.Success(xs)) => Some(xs)
          case Some(scala.util.Failure(_))  => None
          case None                         => None
        }
      })
      .toSet

    if (sets.size > 0) {
      valueChosen = true
    }
    sets
  }

  override def generateCommand(caspaxos: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      caspaxos.numClients -> {
        for {
          clientId <- Gen.choose(0, caspaxos.numClients - 1)
          ints <- Gen.containerOf[Set, Int](Gen.chooseNum(0, 1000000))
        } yield Propose(clientId, ints)
      }
    )

    FakeTransport
      .generateCommandWithFrequency(caspaxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(caspaxos: System, command: Command): System = {
    command match {
      case Propose(clientId, ints) =>
        caspaxos.futures += caspaxos.clients(clientId).propose(ints)
      case TransportCommand(command) =>
        FakeTransport.runCommand(caspaxos.transport, command)
    }
    caspaxos
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    val sorted = state.toSeq.sortBy(_.size)
    for ((x, y) <- sorted.dropRight(1).zip(sorted.drop(1))) {
      if (!x.subsetOf(x)) {
        return SimulatedSystem.InvariantViolated(
          s"Chosen set $x is not a prefix of $y."
        )
      }
    }
    SimulatedSystem.InvariantHolds
  }

  def commandToString(command: Command): String = {
    val caspaxos = newSystem(System.currentTimeMillis())
    command match {
      case Propose(clientIndex, ints) =>
        val clientAddress = caspaxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $ints)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = caspaxos.transport.actors(msg.dst)
        val s = dstActor.serializer.toPrettyString(
          dstActor.serializer.fromBytes(msg.bytes.to[Array])
        )
        s"DeliverMessage(src=${msg.src.address}, dst=${msg.dst.address})\n$s"

      case TransportCommand(FakeTransport.TriggerTimer(address, name, id)) =>
        s"TriggerTimer(${address.address}:$name ($id))"
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
