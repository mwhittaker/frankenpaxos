package frankenpaxos.epaxos

import InstanceHelpers.instanceOrdering
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.GetRequest
import frankenpaxos.statemachine.KeyValueStore
import frankenpaxos.statemachine.KeyValueStoreInput
import frankenpaxos.statemachine.SetKeyValuePair
import frankenpaxos.statemachine.SetRequest
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class EPaxos(val f: Int) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = f + 1
  val numLeaders = 2 * f + 1

  // Configuration.
  val config = Config[FakeTransport](
    f = f,
    leaderAddresses = for (i <- 1 to numLeaders)
      yield FakeTransportAddress(s"Leader $i")
  )

  // Clients.
  val clients = for (i <- 1 to numClients)
    yield
      new Client[FakeTransport](FakeTransportAddress(s"Client $i"),
                                transport,
                                logger,
                                config,
                                options = ClientOptions.default,
                                metrics = new ClientMetrics(FakeCollectors))

  // Leaders
  val leaders = for (i <- 1 to numLeaders)
    yield
      new Leader[FakeTransport](
        FakeTransportAddress(s"Leader $i"),
        transport,
        logger,
        config,
        stateMachine = new KeyValueStore(),
        dependencyGraph = new JgraphtDependencyGraph[Instance, Int](),
        options = LeaderOptions.default,
        metrics = new LeaderMetrics(FakeCollectors)
      )
}

object SimulatedEPaxos {
  sealed trait Command
  case class Propose(
      clientIndex: Int,
      clientPseudonym: Int,
      value: KeyValueStoreInput
  ) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedEPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedEPaxos._

  override type System = EPaxos
  // TODO(mwhittaker): Implement.
  override type State = Map[Instance, Set[Leader.CommandTriple]]
  override type Command = SimulatedEPaxos.Command

  // True if some value has been chosen in some execution of the system.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System = new EPaxos(f)

  override def getState(epaxos: System): State = {
    // Merge two States together, taking a pairwise union.
    def merge(lhs: State, rhs: State): State = {
      val merged = for (k <- lhs.keys ++ rhs.keys)
        yield {
          k -> lhs.getOrElse(k, Set()).union(rhs.getOrElse(k, Set()))
        }
      Map(merged.toSeq: _*)
    }

    // We look at the commands recorded chosen by the leaders.
    val chosen = epaxos.leaders
      .map(leader => Map() ++ leader.cmdLog)
      .map(cmdLog => {
        cmdLog.flatMap({
          case (i, Leader.CommittedEntry(triple)) => Some(i -> triple)
          case _                                  => None
        })
      })
      .map(cmdLog => cmdLog.mapValues(Set[Leader.CommandTriple](_)))
      .foldLeft(Map[Instance, Set[Leader.CommandTriple]]())(merge(_, _))
    if (chosen.size > 0) {
      valueChosen = true
    }
    chosen
  }

  override def generateCommand(epaxos: System): Option[Command] = {
    val keys = Seq("a", "b", "c", "d")
    val keyValues = keys.map((_, "value"))

    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      epaxos.numClients -> {
        for {
          clientId <- Gen.choose(0, epaxos.numClients - 1)
          clientPseudonym <- Gen.choose(0, 2)
          request <- Gen.oneOf(KeyValueStore.getOneOf(keys),
                               KeyValueStore.setOneOf(keyValues))
        } yield Propose(clientId, clientPseudonym, request)
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
      case Propose(clientId, clientPseudonym, request) =>
        epaxos.clients(clientId).propose(clientPseudonym, request.toByteArray)
      case TransportCommand(command) =>
        FakeTransport.runCommand(epaxos.transport, command)
    }
    epaxos
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    // Every instance has a single committed entry.
    for ((instance, chosen) <- state) {
      if (chosen.size > 1) {
        return SimulatedSystem.InvariantViolated(
          s"Instance $instance has multiple chosen values: $chosen."
        )
      }
    }

    // Every pair of conflicting instances has a dependency on each other.
    for ((instanceA, chosenA) <- state if chosenA.size > 0) {
      for {
        (instanceB, chosenB) <- state
        if instanceA != instanceB
        if chosenB.size > 0
      } {
        val Leader.CommandTriple(commandOrNoopA, _, depsA) = chosenA.head
        val Leader.CommandTriple(commandOrNoopB, _, depsB) = chosenB.head

        import CommandOrNoop.Value
        (commandOrNoopA.value, commandOrNoopB.value) match {
          case (Value.Command(commandA), Value.Command(commandB)) =>
            if (new KeyValueStore().conflicts(commandA.command.toByteArray,
                                              commandB.command.toByteArray) &&
                !depsA.contains(instanceB) &&
                !depsB.contains(instanceA)) {
              return SimulatedSystem.InvariantViolated(
                s"Instances $instanceA and $instanceB conflict but do not " +
                  s"depend on each other (dependencies $depsA and $depsB)."
              )
            }

          case (Value.Empty, _) | (_, Value.Empty) =>
            return SimulatedSystem.InvariantViolated(
              s"Empty CommandOrNoop found."
            )

          case (Value.Noop(_), _) | (_, Value.Noop(_)) =>
          // Nothing to check.
        }
      }
    }

    SimulatedSystem.InvariantHolds
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    for (instance <- oldState.keys ++ newState.keys) {
      val oldChosen = oldState.getOrElse(instance, Set[Leader.CommandTriple]())
      val newChosen = newState.getOrElse(instance, Set[Leader.CommandTriple]())
      if (!oldChosen.subsetOf(newChosen)) {
        SimulatedSystem.InvariantViolated(
          s"Instance $instance was $oldChosen but now is $newChosen."
        )
      }
    }
    SimulatedSystem.InvariantHolds
  }

  def commandToString(command: Command): String = {
    val epaxos = newSystem(System.currentTimeMillis())
    command match {
      case Propose(clientIndex, clientPseudonym, value) =>
        val clientAddress = epaxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $clientPseudonym, $value)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = epaxos.transport.actors(msg.dst)
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
