package frankenpaxos.fastmultipaxos

import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.roundsystem.RoundSystem
import frankenpaxos.simulator.FakeLogger
import frankenpaxos.simulator.FakeTransport
import frankenpaxos.simulator.FakeTransportAddress
import frankenpaxos.simulator.FakeTransportMessage
import frankenpaxos.simulator.FakeTransportTimer
import frankenpaxos.simulator.SimulatedSystem
import frankenpaxos.statemachine.AppendLog
import org.scalacheck
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.collection.mutable

class FastMultiPaxos(
    val f: Int,
    val roundSystem: RoundSystem
) {
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
  val leaders = for (i <- 1 to numLeaders) yield {
    val options = LeaderOptions.default.copy(
      thriftySystem = ThriftySystem.Random,
      phase2aMaxBufferSize = 2,
      valueChosenMaxBufferSize = 2
    )
    new Leader[FakeTransport](FakeTransportAddress(s"Leader $i"),
                              transport,
                              logger,
                              config,
                              new AppendLog(),
                              options,
                              new LeaderMetrics(FakeCollectors))
  }

  // Acceptors.
  val acceptors = for (i <- 1 to numAcceptors) yield {
    val options = AcceptorOptions.default.copy(
      waitPeriod = java.time.Duration.ofMillis(10),
      waitStagger = java.time.Duration.ofMillis(10)
    )
    new Acceptor[FakeTransport](FakeTransportAddress(s"Acceptor $i"),
                                transport,
                                logger,
                                config,
                                options,
                                new AcceptorMetrics(FakeCollectors))
  }
}

object SimulatedFastMultiPaxos {
  sealed trait Command
  case class Propose(clientIndex: Int, clientPseudonym: Int, value: String)
      extends Command
  case class TransportCommand(command: FakeTransport.Command) extends Command
}

class SimulatedFastMultiPaxos(
    val f: Int,
    val roundSystem: RoundSystem
) extends SimulatedSystem {
  import SimulatedFastMultiPaxos._

  type Slot = Int
  override type System = FastMultiPaxos
  // The state of FastMultiPaxos records the set of chosen entries for every
  // log slot. Every set should be empty or a singleton.
  override type State = collection.SortedMap[Slot, Set[Leader.Entry]]
  override type Command = SimulatedFastMultiPaxos.Command

  var valueChosen: Boolean = false

  override def newSystem(): System = new FastMultiPaxos(f, roundSystem)

  override def getState(fastMultiPaxos: System): State = {
    // Merge two States together, taking a pairwise union.
    def merge(lhs: State, rhs: State): State = {
      val merged = for (k <- lhs.keys ++ rhs.keys)
        yield {
          k -> lhs.getOrElse(k, Set()).union(rhs.getOrElse(k, Set()))
        }
      collection.SortedMap(merged.toSeq: _*)
    }

    // We look at the commands recorded chosen by the leaders.
    val chosen = fastMultiPaxos.leaders
      .map(leader => leader.log.mapValues(Set[Leader.Entry](_)))
      .foldLeft(collection.SortedMap[Slot, Set[Leader.Entry]]())(merge(_, _))
    if (chosen.size > 0) {
      valueChosen = true
    }
    chosen
  }

  override def generateCommand(fastMultiPaxos: System): Option[Command] = {
    // Generating commands for Fast MultiPaxos can get a bit tricky. Fast
    // MultiPaxos includes leader election and hearbeating. If we're not
    // careful, the protocol runs those subprotocols too often and no real work
    // gets done. Thus, we separate out the "good" messages and timers (i.e.
    // the ones from clients, leaders, and acceptors) from the "bad" messages
    // and timers (i.e. the ones from the leader election and heartbeat
    // subprotocols.)
    //
    // We also weight message delivery more than everything else because we
    // don't want too many timers triggering or too many proposes happening.
    // Empirically, when this happens, the protocol doesn't choose very much
    // stuff.

    val goodAddresses = {
      fastMultiPaxos.clients.map(_.address) ++
        fastMultiPaxos.leaders.map(_.address) ++
        fastMultiPaxos.acceptors.map(_.address)
    }.toSet

    def goodMessage(msg: FakeTransportMessage): Boolean =
      goodAddresses.contains(msg.src) && goodAddresses.contains(msg.dst)

    def goodTimer(timer: FakeTransportTimer): Boolean =
      goodAddresses.contains(timer.address)

    val transport = fastMultiPaxos.transport
    val goodMessages = transport.messages.filter(goodMessage)
    val badMessages = transport.messages.filter(!goodMessage(_))
    val goodTimers = transport.runningTimers().filter(goodTimer)
    val badTimers = transport.runningTimers().filter(!goodTimer(_))

    val subgens = mutable.Buffer[(Int, Gen[Command])]()

    // Propose.
    subgens += fastMultiPaxos.numClients -> {
      for {
        clientId <- Gen.choose(0, fastMultiPaxos.numClients - 1)
        clientPseudonym <- Gen.choose(0, 1)
        value <- Gen.listOfN(10, Gen.alphaLowerChar).map(_.mkString(""))
      } yield Propose(clientId, clientPseudonym, value)
    }

    // Good messages.
    if (goodMessages.size > 0) {
      subgens += 50 * goodMessages.size ->
        Gen
          .oneOf(goodMessages)
          .map(m => TransportCommand(FakeTransport.DeliverMessage(m)))
    }

    // Good timers.
    if (goodTimers.size > 0) {
      subgens += goodTimers.size ->
        Gen
          .oneOf(goodTimers.toSeq)
          .map({ t =>
            TransportCommand(
              FakeTransport.TriggerTimer(address = t.address,
                                         name = t.name(),
                                         timerId = t.id)
            )
          })
    }

    // Bad messages and timers.
    if (badMessages.size > 0) {
      subgens += 1 ->
        Gen
          .oneOf(badMessages)
          .map(m => TransportCommand(FakeTransport.DeliverMessage(m)))
    }

    // Bad timers.
    if (badTimers.size > 0) {
      subgens += 1 ->
        Gen
          .oneOf(badTimers.toSeq)
          .map({ t =>
            TransportCommand(
              FakeTransport.TriggerTimer(address = t.address,
                                         name = t.name(),
                                         timerId = t.id)
            )
          })
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

  override def stateInvariantHolds(
      state: State
  ): SimulatedSystem.InvariantResult = {
    for ((slot, chosen) <- state) {
      if (chosen.size > 1) {
        return SimulatedSystem.InvariantViolated(
          s"Slot $slot has multiple chosen values: $chosen."
        )
      }
    }
    SimulatedSystem.InvariantHolds
  }

  override def stepInvariantHolds(
      oldState: State,
      newState: State
  ): SimulatedSystem.InvariantResult = {
    for (slot <- oldState.keys ++ newState.keys) {
      val oldChosen = oldState.getOrElse(slot, Set[Leader.Entry]())
      val newChosen = newState.getOrElse(slot, Set[Leader.Entry]())
      if (!oldChosen.subsetOf(newChosen)) {
        SimulatedSystem.InvariantViolated(
          s"Slot $slot was $oldChosen but now is $newChosen."
        )
      }
    }
    SimulatedSystem.InvariantHolds
  }

  def commandToString(command: Command): String = {
    val fastMultiPaxos = newSystem()
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
