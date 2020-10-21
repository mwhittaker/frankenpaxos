package frankenpaxos.sum

import frankenpaxos.Util
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.ReadableAppendLog
import frankenpaxos.util
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class Sum {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2

  // Clients.
  val clients = for (i <- 1 to numClients) yield {
    new Client[FakeTransport](
      srcAddress = FakeTransportAddress(s"Client $i"),
      dstAddress = FakeTransportAddress("Server"),
      transport = transport,
      logger = new FakeLogger()
    )
  }

  // Server.
  val server = new Server[FakeTransport](
    address = FakeTransportAddress("Server"),
    transport = transport,
    logger = new FakeLogger(),
    metrics = new ServerMetrics(FakeCollectors)
  )
}

object SimulatedSum {
  sealed trait Command

  case class Add(
      clientIndex: Int,
      x: Int
  ) extends Command

  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedSum extends SimulatedSystem {
  import SimulatedSum._

  override type System = Sum
  // For every replica, we record the prefix of the log that has been executed.
  override type State = Int
  override type Command = SimulatedSum.Command

  override def newSystem(seed: Long): System = new Sum()

  override def getState(sum: System): State = sum.server.runningSum

  override def generateCommand(sum: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Sum.
      sum.numClients -> {
        for {
          clientId <- Gen.choose(0, sum.numClients - 1)
          x <- Gen.choose(0, 100)
        } yield Add(clientId, x)

      }
    )
    FakeTransport
      .generateCommandWithFrequency(sum.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(sum: System, command: Command): System = {
    command match {
      case Add(clientId, x) =>
        sum.clients(clientId).add(x)
      case TransportCommand(command) =>
        FakeTransport.runCommand(sum.transport, command)
    }
    sum
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    if (state < 0) {
      SimulatedSystem.InvariantViolated(s"State $state is negative.")
    } else {
      SimulatedSystem.InvariantHolds
    }
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    if (oldState > newState) {
      SimulatedSystem.InvariantViolated(
        s"Old sum $oldState is less than or equal to $newState."
      )
    } else {
      SimulatedSystem.InvariantHolds
    }
  }

  def commandToString(command: Command): String = {
    val sum = newSystem(System.currentTimeMillis())

    command match {
      case Add(clientIndex, x) =>
        val clientAddress = sum.clients(clientIndex).address.address
        s"Add($clientAddress, $x)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = sum.transport.actors(msg.dst)
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
