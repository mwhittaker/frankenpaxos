package frankenpaxos.unanimousbpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.Util
import frankenpaxos.Util.MapHelpers
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.GetRequest
import frankenpaxos.statemachine.KeyValueStore
import frankenpaxos.statemachine.KeyValueStoreInput
import frankenpaxos.statemachine.SetRequest
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class UnanimousBPaxos(val f: Int) {
  val logger = new FakeLogger()
  val transport = new FakeTransport(logger)
  val numClients = 2 * f + 1
  val numLeaders = f + 1
  val numDepServiceNodes = 2 * f + 1
  val numAcceptors = 2 * f + 1

  // Configuration.
  val config = Config[FakeTransport](
    f = f,
    leaderAddresses = for (i <- 1 to numLeaders)
      yield FakeTransportAddress(s"Leader $i"),
    depServiceNodeAddresses = for (i <- 1 to numDepServiceNodes)
      yield FakeTransportAddress(s"Dep Service Node $i"),
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
      options = ClientOptions.default,
      metrics = new ClientMetrics(FakeCollectors)
    )
  }

  // Leaders.
  val leaders = for (i <- 1 to numLeaders) yield {
    new Leader[FakeTransport](
      address = FakeTransportAddress(s"Leader $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      stateMachine = new KeyValueStore(),
      dependencyGraph = new JgraphtDependencyGraph(),
      options = LeaderOptions.default,
      metrics = new LeaderMetrics(FakeCollectors)
    )
  }

  // DepServiceNodes.
  val depServiceNodes = for (i <- 1 to numDepServiceNodes) yield {
    new DepServiceNode[FakeTransport](
      address = FakeTransportAddress(s"Dep Service Node $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      stateMachine = new KeyValueStore(),
      options = DepServiceNodeOptions.default,
      metrics = new DepServiceNodeMetrics(FakeCollectors)
    )
  }

  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors) yield {
    new Acceptor[FakeTransport](
      address = FakeTransportAddress(s"Acceptor $i"),
      transport = transport,
      logger = new FakeLogger(),
      config = config,
      options = AcceptorOptions.default,
      metrics = new AcceptorMetrics(FakeCollectors)
    )
  }
}

object SimulatedUnanimousBPaxos {
  sealed trait Command
  case class Propose(
      clientIndex: Int,
      clientPseudonym: Int,
      value: KeyValueStoreInput
  ) extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedUnanimousBPaxos(val f: Int) extends SimulatedSystem {
  import SimulatedUnanimousBPaxos._

  override type System = UnanimousBPaxos
  // For each vertex id, we record the set of chosen VoteValues. If everything
  // is correct, every set should contain at most one vote value.
  override type State = Map[VertexId, Set[Acceptor.VoteValue]]
  override type Command = SimulatedUnanimousBPaxos.Command

  // True if some value has been chosen in some execution of the system.
  var valueChosen: Boolean = false

  override def newSystem(seed: Long): System = new UnanimousBPaxos(f)

  override def getState(bpaxos: System): State = {
    // Merge two States together, taking a pairwise union.
    def merge(lhs: State, rhs: State): State = {
      lhs.merge(rhs) {
        case (_, Util.Left(l))    => l
        case (_, Util.Both(l, r)) => l.union(r)
        case (_, Util.Right(r))   => r
      }
    }

    // We look at the commands recorded chosen by the leaders.
    val chosen = bpaxos.leaders
      .map(leader => Map() ++ leader.states)
      .map(states => {
        states.flatMap({
          case (i, Leader.Committed(c, d)) =>
            Some(i -> Set(Acceptor.VoteValue(c, d)))
          case _ =>
            None
        })
      })
      .foldLeft(Map[VertexId, Set[Acceptor.VoteValue]]())(merge(_, _))

    if (chosen.size > 0) {
      valueChosen = true
    }
    chosen
  }

  override def generateCommand(bpaxos: System): Option[Command] = {
    val keys = Seq("a", "b", "c", "d")
    val keyValues = keys.map((_, "value"))

    val subgens = mutable.Buffer[(Int, Gen[Command])](
      // Propose.
      bpaxos.numClients -> {
        for {
          clientId <- Gen.choose(0, bpaxos.numClients - 1)
          clientPseudonym <- Gen.choose(0, 2)
          request <- Gen.oneOf(KeyValueStore.getOneOf(keys),
                               KeyValueStore.setOneOf(keyValues))
        } yield Propose(clientId, clientPseudonym, request)
      }
    )
    FakeTransport
      .generateCommandWithFrequency(bpaxos.transport)
      .foreach({
        case (frequency, gen) =>
          subgens += frequency -> gen.map(TransportCommand(_))
      })

    val gen: Gen[Command] = Gen.frequency(subgens: _*)
    gen.apply(Gen.Parameters.default, Seed.random())
  }

  override def runCommand(bpaxos: System, command: Command): System = {
    command match {
      case Propose(clientId, clientPseudonym, request) =>
        bpaxos.clients(clientId).propose(clientPseudonym, request.toByteArray)
      case TransportCommand(command) =>
        FakeTransport.runCommand(bpaxos.transport, command)
    }
    bpaxos
  }

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    // Every vertexId has a single committed entry.
    for ((vertexId, chosen) <- state) {
      if (chosen.size > 1) {
        return SimulatedSystem.InvariantViolated(
          s"Vertex $vertexId has multiple chosen values: $chosen."
        )
      }
    }

    // Every pair of conflicting vertices has a dependency on each other.
    val chosens = state.filter({ case (_, chosen) => chosen.size > 1 })
    for ((vertexA, chosenA) <- chosens) {
      for ((vertexB, chosenB) <- chosens if vertexA != vertexB) {
        val Acceptor.VoteValue(commandOrNoopA, depsA) = chosenA.head
        val Acceptor.VoteValue(commandOrNoopB, depsB) = chosenB.head

        import CommandOrNoop.Value._
        (commandOrNoopA.value, commandOrNoopB.value) match {
          case (Command(commandA), Command(commandB)) =>
            val bytesA = commandA.command.toByteArray
            val bytesB = commandB.command.toByteArray
            if (new KeyValueStore().conflicts(bytesA, bytesB) &&
                !depsA.contains(vertexB) &&
                !depsB.contains(vertexA)) {
              return SimulatedSystem.InvariantViolated(
                s"Vertices $vertexA and $vertexB conflict but do not " +
                  s"depend on each other (dependencies $depsA and $depsB)."
              )
            }

          case (Empty, _) | (_, Empty) =>
            return SimulatedSystem.InvariantViolated(
              s"Empty CommandOrNoop found."
            )

          case (Noop(_), _) | (_, Noop(_)) =>
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
    // Check that sets of chosen values only grow over time.
    for (vertexId <- oldState.keys ++ newState.keys) {
      val oldChosen = oldState.getOrElse(vertexId, Set[Acceptor.VoteValue]())
      val newChosen = newState.getOrElse(vertexId, Set[Acceptor.VoteValue]())
      if (!oldChosen.subsetOf(newChosen)) {
        SimulatedSystem.InvariantViolated(
          s"Vertex $vertexId was $oldChosen but now is $newChosen."
        )
      }
    }
    SimulatedSystem.InvariantHolds
  }

  def commandToString(command: Command): String = {
    val bpaxos = newSystem(System.currentTimeMillis())
    command match {
      case Propose(clientIndex, clientPseudonym, value) =>
        val clientAddress = bpaxos.clients(clientIndex).address.address
        s"Propose($clientAddress, $clientPseudonym, $value)"

      case TransportCommand(FakeTransport.DeliverMessage(msg)) =>
        val dstActor = bpaxos.transport.actors(msg.dst)
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
