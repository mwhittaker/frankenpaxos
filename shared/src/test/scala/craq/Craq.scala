package craq

import frankenpaxos.craq.{ChainNode, ChainNodeMetrics, ChainNodeOptions, Client, ClientMetrics, ClientOptions, Config, Hash}
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator.{FakeLogger, FakeTransport, FakeTransportAddress, SimulatedSystem}
import frankenpaxos.statemachine.ReadableAppendLog
import org.scalacheck.Gen
import org.scalacheck.rng.Seed

import scala.collection.mutable

class Craq(val f: Int, batched: Boolean, seed: Long) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2
  val numBatchers = if (batched) {
    f + 1
  } else {
    0
  }
  val numChainNodes = 2 * f + 1

  val config = Config[FakeTransport](
    f,
    chainNodeAddresses = (1 to numChainNodes).map(i => FakeTransportAddress(s"ChainNode $i")),
    distributionScheme = Hash,
    numBatchers
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

  // ChainNodes
  val chainNodes = for (address <- config.chainNodeAddresses) yield {
    new ChainNode[FakeTransport](
      address = address,
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = ChainNodeOptions.default,
      metrics = new ChainNodeMetrics(FakeCollectors),
      seed = seed
    )
  }
}

object SimulatedCraq {
  sealed trait Command

  case class Write(
      clientIndex: Int,
      clientPseudonym: Int,
      key: String,
      value: String
  ) extends Command

  case class Read(
      clientIndex: Int,
      clientPseudonym: Int,
      key: String
  ) extends Command

  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedCraq(val f: Int, batched: Boolean)
    extends SimulatedSystem {
  import SimulatedCraq._

  override type System = Craq
  // For every replica, we record the prefix of the log that has been executed.
  override type State = mutable.Buffer[mutable.Map[String, String]]
  override type Command = craq.SimulatedCraq.Command

  // True if some value has been chosen in some execution of the system. Seeing
  // whether any value has been chosen is a very coarse way of testing
  // liveness. If no value is every chosen, then clearly something is wrong.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System = new Craq(f, batched, seed)

  override def getState(craq: System): State = {
    val logs = mutable.Buffer[mutable.Map[String, String]]()
    for (chainNode <- craq.chainNodes) {
      if (chainNode.versions > 0) {
        valueChosen = true
      }
      logs += (chainNode.stateMachine)
    }
    logs
  }

  override def generateCommand(craq: System): Option[Command] = {
    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Write.
      craq.numClients * 3 -> {
        for {
          clientId <- Gen.choose(0, craq.numClients - 1)
          key <- Gen.alphaLowerStr.filter(_.size > 0)
          value <- Gen.alphaLowerStr.filter(_.size > 0)
        } yield Write(clientId, clientPseudonym = 0, key, value)

      },
      // Read.
      /*craq.numClients -> {
        for {
          clientId <- Gen.choose(0, craq.numClients - 1)
          key <- Gen.alphaLowerStr.filter(_.size > 0)
        } yield Read(clientId, clientPseudonym = 0, key)
      },*/
    )
    FakeTransport
      .generateCommandWithFrequency(craq.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(craq: System, command: Command): System = {
    command match {
      case Write(clientId, clientPseudonym, key, value) =>
        craq.clients(clientId).write(clientPseudonym, key, value)
      case Read(clientId, clientPseudonym, key) =>
        craq.clients(clientId).read(clientPseudonym, key)
      case TransportCommand(command) =>
        FakeTransport.runCommand(craq.transport, command)
    }
    craq
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    for (logs <- state.combinations(2)) {
      val lhs = logs(0)
      val rhs = logs(1)

      for (key <- lhs.keys) {
        if (!lhs.get(key).get.equalsIgnoreCase(rhs.get(key).get)) {
          return SimulatedSystem.InvariantViolated(s"Logs $lhs and $rhs are not compatible.")
        }
      }
    }

    SimulatedSystem.InvariantHolds
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    for ((oldLog, newLog) <- oldState.zip(newState)) {
      if (oldLog.size < newLog.size) {
        return SimulatedSystem.InvariantViolated(s"Old kvs $oldLog is not a prefix of new kvs $newLog")
      }
    }
    SimulatedSystem.InvariantHolds
  }

  def commandToString(command: Command): String = {
    val craq = newSystem(System.currentTimeMillis())
    command match {
      case Write(clientIndex, clientPseudonym, key, value) =>
        val clientAddress = craq.clients(clientIndex).address
        s"Write($clientAddress, $clientPseudonym, $key, $value)"

      case Read(clientIndex, clientPseudonym, key) =>
        val clientAddress = craq.clients(clientIndex).address
        s"Read($clientAddress, $clientPseudonym, $key)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = craq.transport.actors(msg.dst)
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
