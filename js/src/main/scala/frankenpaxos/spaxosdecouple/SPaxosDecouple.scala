package frankenpaxos.spaxosdecouple

import frankenpaxos.{JsLogger, JsTransport, JsTransportAddress, roundsystem}
import frankenpaxos.monitoring.FakeCollectors
import frankenpaxos.statemachine.AppendLog

import scala.scalajs.js.annotation._

@JSExportAll
class SPaxosDecouple {
  // Transport.
  val logger = new JsLogger()
  val transport = new JsTransport(logger);

  // Configuration.
  val config = Config[JsTransport](
    f = 1,
    proposerAddresses = List(
      JsTransportAddress("Proposer 1"),
      JsTransportAddress("Proposer 2"),
      JsTransportAddress("Proposer 3")
    ),
    leaderAddresses = List(
      JsTransportAddress("Leader 1")
    ),
    acceptorAddresses = List(
      JsTransportAddress("Acceptor 1"),
      JsTransportAddress("Acceptor 2"),
      JsTransportAddress("Acceptor 3")
    ),
    acceptorHeartbeatAddresses = List(
      JsTransportAddress("AcceptorHeartbeat 1"),
      JsTransportAddress("AcceptorHeartbeat 2"),
      JsTransportAddress("AcceptorHeartbeat 3")
    ),
    leaderHeartbeatAddresses = List(
      JsTransportAddress("LeaderHeartbeat 1")
    ),
    leaderElectionAddresses = List(
      JsTransportAddress("LeaderElection 1")
    ),
    executorAddresses = List(
      JsTransportAddress("Executor 1"),
      JsTransportAddress("Executor 2"),
      JsTransportAddress("Executor 3")
    ),
    roundSystem = new roundsystem.RoundSystem.ClassicRoundRobin(1)
  )

  // Clients.
  val clients = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Client $i")
    val client = new Client[JsTransport](address,
                                         transport,
                                         logger,
                                         config,
                                         ClientOptions.default,
                                         new ClientMetrics(FakeCollectors))
    (logger, client)
  }
  val (client1logger, client1) = clients(0)
  val (client2logger, client2) = clients(1)
  val (client3logger, client3) = clients(2)

  // Leaders.
  val leaders = for (i <- 1 to 1) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Leader $i")
    val leader = new Leader[JsTransport](address,
                                         transport,
                                         logger,
                                         config,
      new AppendLog(),
      LeaderOptions.default, new LeaderMetrics(FakeCollectors))
    (logger, leader)
  }
  val (leader1logger, leader1) = leaders(0)

  // Acceptors.
  val acceptorOptions = AcceptorOptions(
    waitPeriod = java.time.Duration.ofMillis(500),
    waitStagger = java.time.Duration.ofMillis(500)
  )
  val acceptors = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Acceptor $i")
    val acceptor = new Acceptor[JsTransport](
      address,
      transport,
      logger,
      config,
      acceptorOptions,
      new AcceptorMetrics(FakeCollectors)
    )
    (logger, acceptor)
  }
  val (acceptor1logger, acceptor1) = acceptors(0)
  val (acceptor2logger, acceptor2) = acceptors(1)
  val (acceptor3logger, acceptor3) = acceptors(2)

  // Proposers.
  val proposerOptions = ProposerOptions.default

  val proposers = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Proposer $i")
    val proposer = new Proposer[JsTransport](
      address,
      transport,
      logger,
      config,
      proposerOptions,
      new ProposerMetrics(FakeCollectors)
    )
    (logger, proposer)
  }
  val (proposer1logger, proposer1) = proposers(0)
  val (proposer2logger, proposer2) = proposers(1)
  val (proposer3logger, proposer3) = proposers(2)

  // Executors.
  val executorOptions = ExecutorOptions.default

  val executors = for (i <- 1 to 3) yield {
    val logger = new JsLogger()
    val address = JsTransportAddress(s"Executor $i")
    val executor = new Executor[JsTransport](
      address,
      transport,
      logger,
      config,
      new AppendLog(),
      executorOptions,
      new ExecutorMetrics(FakeCollectors)
    )
    (logger, executor)
  }
  val (executor1logger, executor1) = executors(0)
  val (executor2logger, executor2) = executors(1)
  val (executor3logger, executor3) = executors(2)
}

@JSExportAll
@JSExportTopLevel("frankenpaxos.spaxosdecouple.TweenedSPaxosDecouple")
object TweenedSPaxosDecouple {
  val SPaxosDecouple = new SPaxosDecouple()
}
