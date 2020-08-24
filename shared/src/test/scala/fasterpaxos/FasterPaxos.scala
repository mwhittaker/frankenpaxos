package frankenpaxos.fasterpaxos

import frankenpaxos.Util
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.AppendLog
import frankenpaxos.util
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class FasterPaxos(
    val f: Int,
    ackNoopsWithCommands: Boolean,
    useF1Optimization: Boolean,
    seed: Long
) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2 * f + 1
  val numServers = 2 * f + 1

  val config = Config[FakeTransport](
    f = f,
    serverAddresses =
      (1 to numServers).map(i => FakeTransportAddress(s"Server $i")),
    heartbeatAddresses =
      (1 to numServers).map(i => FakeTransportAddress(s"Heartbeat $i"))
  )

  // Clients.
  val clients = for (i <- 1 to numClients) yield {
    new Client[FakeTransport](
      address = FakeTransportAddress(s"Client $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ClientOptions.default,
      metrics = new ClientMetrics(FakeCollectors),
      seed = seed
    )
  }

  // Servers.
  val servers = for (address <- config.serverAddresses) yield {
    new Server[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      stateMachine = new AppendLog(),
      config = config,
      options = ServerOptions.default.copy(
        ackNoopsWithCommands = ackNoopsWithCommands,
        logGrowSize = 10,
        useF1Optimization = (f == 1) && useF1Optimization
      ),
      metrics = new ServerMetrics(FakeCollectors),
      seed = seed
    )
  }
}

object SimulatedFasterPaxos {
  sealed trait Command

  case class Write(
      clientIndex: Int,
      clientPseudonym: Int,
      value: String
  ) extends Command

  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedFasterPaxos(
    val f: Int,
    ackNoopsWithCommands: Boolean,
    useF1Optimization: Boolean
) extends SimulatedSystem {
  import SimulatedFasterPaxos._

  override type System = FasterPaxos
  // For every server, we record the prefix of the log that has been executed.
  override type State = mutable.Buffer[Seq[CommandOrNoop]]
  override type Command = SimulatedFasterPaxos.Command

  // True if some value has been chosen in some execution of the system. Seeing
  // whether any value has been chosen is a very coarse way of testing
  // liveness. If no value is every chosen, then clearly something is wrong.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System =
    new FasterPaxos(f, ackNoopsWithCommands, useF1Optimization, seed)

  override def getState(paxos: System): State = {
    val logs = mutable.Buffer[Seq[CommandOrNoop]]()
    for (server <- paxos.servers) {
      if (server.executedWatermark > 0) {
        valueChosen = true
      }
      logs += (0 until server.executedWatermark).map(i => {
        server.log.get(i) match {
          case None | Some(_: server.PendingEntry) =>
            throw new IllegalStateException(
              s"A server has a non-chosen log entry below its " +
                s"executedWatermark (${server.executedWatermark}): " +
                s"${server.log}"
            )

          case Some(chosen: server.ChosenEntry) =>
            chosen.value
        }
      })
    }

    logs
  }

  override def generateCommand(paxos: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Write.
      paxos.numClients -> {
        for {
          clientId <- Gen.choose(0, paxos.numClients - 1)
          request <- Gen.alphaLowerStr.filter(_.size > 0)
        } yield Write(clientId, clientPseudonym = 0, request)

      }
    )
    FakeTransport
      .generateCommandWithFrequency(paxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(paxos: System, command: Command): System = {
    command match {
      case Write(clientId, clientPseudonym, request) =>
        paxos.clients(clientId).write(clientPseudonym, request)
      case TransportCommand(command) =>
        FakeTransport.runCommand(paxos.transport, command)
    }
    paxos
  }

  private def isPrefix[A](lhs: Seq[A], rhs: Seq[A]): Boolean = {
    lhs.zipWithIndex.forall({
      case (x, i) => rhs.lift(i) == Some(x)
    })
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    for (logs <- state.combinations(2)) {
      val lhs = logs(0)
      val rhs = logs(1)
      if (!isPrefix(lhs, rhs) && !isPrefix(rhs, lhs)) {
        return SimulatedSystem.InvariantViolated(
          s"Logs $lhs and $rhs are not compatible."
        )
      }
    }

    SimulatedSystem.InvariantHolds
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    for ((oldLog, newLog) <- oldState.zip(newState)) {
      if (!isPrefix(oldLog, newLog)) {
        return SimulatedSystem.InvariantViolated(
          s"Logs $oldLog is not a prefix of $newLog."
        )
      }
    }

    SimulatedSystem.InvariantHolds
  }

  def commandToString(command: Command): String = {
    val paxos = newSystem(System.currentTimeMillis())
    command match {
      case Write(clientIndex, clientPseudonym, value) =>
        val clientAddress = paxos.clients(clientIndex).address.address
        s"Write($clientAddress, $clientPseudonym, $value)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = paxos.transport.actors(msg.dst)
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