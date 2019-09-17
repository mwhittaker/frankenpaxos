package frankenpaxos.simplebpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.ZigzagTarjanDependencyGraph
import frankenpaxos.depgraph.ZigzagTarjanDependencyGraphOptions
import frankenpaxos.statemachine
import frankenpaxos.statemachine.KeyValueStore
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.thrifty.ThriftySystem
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration

object SuperNodeMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      stateMachine: StateMachine = new statemachine.Noop(),
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      leaderOptions: LeaderOptions = LeaderOptions.default,
      depServiceNodeOptions: DepServiceNodeOptions =
        DepServiceNodeOptions.default,
      proposerOptions: ProposerOptions = ProposerOptions.default,
      acceptorOptions: AcceptorOptions = AcceptorOptions.default,
      replicaOptions: ReplicaOptions = ReplicaOptions.default,
      zigzagOptions: ZigzagTarjanDependencyGraphOptions =
        ZigzagTarjanDependencyGraphOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def leaderOptionAction(
        f: (A, LeaderOptions) => LeaderOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(leaderOptions = f(x, flags.leaderOptions))
      )

    def depNodeOptionAction(
        f: (A, DepServiceNodeOptions) => DepServiceNodeOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) =>
          flags.copy(depServiceNodeOptions = f(x, flags.depServiceNodeOptions))
      )

    def proposerOptionAction(
        f: (A, ProposerOptions) => ProposerOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(proposerOptions = f(x, flags.proposerOptions))
      )

    def acceptorOptionAction(
        f: (A, AcceptorOptions) => AcceptorOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(acceptorOptions = f(x, flags.acceptorOptions))
      )

    def replicaOptionAction(
        f: (A, ReplicaOptions) => ReplicaOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(replicaOptions = f(x, flags.replicaOptions))
      )

    def zigzagOptionAction(
        f: (
            A,
            ZigzagTarjanDependencyGraphOptions
        ) => ZigzagTarjanDependencyGraphOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) => flags.copy(zigzagOptions = f(x, flags.zigzagOptions))
      )
  }

  val parser = new scopt.OptionParser[Flags]("") {
    help("help")

    // Basic flags.
    opt[Int]("index").required().action((x, f) => f.copy(index = x))
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))
    opt[StateMachine]("state_machine")
      .required()
      .action((x, f) => f.copy(stateMachine = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"-1 to disable")

    // LeaderOptions.
    opt[ThriftySystem]("leader.thriftySystem")
      .leaderOptionAction((x, o) => o.copy(thriftySystem = x))
    opt[java.time.Duration]("leader.resendDependencyRequestsTimerPeriod")
      .leaderOptionAction(
        (x, o) => o.copy(resendDependencyRequestsTimerPeriod = x)
      )

    // DepServiceNodeOptions.
    opt[Boolean]("depnode.unsafeReturnNoDependencies")
      .depNodeOptionAction((x, o) => o.copy(unsafeReturnNoDependencies = x))
    opt[Int]("depnode.topKDependencies")
      .depNodeOptionAction((x, o) => o.copy(topKDependencies = x))

    // ProposerOptions.
    opt[ThriftySystem]("proposer.thriftySystem")
      .proposerOptionAction((x, o) => o.copy(thriftySystem = x))
    opt[java.time.Duration]("proposer.resendPhase1asTimerPeriod")
      .proposerOptionAction((x, o) => o.copy(resendPhase1asTimerPeriod = x))
    opt[java.time.Duration]("proposer.resendPhase2asTimerPeriod")
      .proposerOptionAction((x, o) => o.copy(resendPhase2asTimerPeriod = x))

    // AcceptorOptions.

    // ReplicaOptions.
    opt[java.time.Duration]("replica.recoverVertexTimerMinPeriod")
      .replicaOptionAction((x, o) => o.copy(recoverVertexTimerMinPeriod = x))
    opt[java.time.Duration]("replica.recoverVertexTimerMaxPeriod")
      .replicaOptionAction((x, o) => o.copy(recoverVertexTimerMaxPeriod = x))
    opt[Boolean]("replica.unsafeSkipGraphExecution")
      .replicaOptionAction((x, o) => o.copy(unsafeSkipGraphExecution = x))
    opt[Int]("replica.executeGraphBatchSize")
      .replicaOptionAction((x, o) => o.copy(executeGraphBatchSize = x))
    opt[java.time.Duration]("replica.executeGraphTimerPeriod")
      .replicaOptionAction((x, o) => o.copy(executeGraphTimerPeriod = x))
    opt[Int]("replica.numBlockers")
      .replicaOptionAction((x, o) => o.copy(numBlockers = x))
    opt[Int]("zigzag.verticesGrowSize")
      .zigzagOptionAction((x, o) => o.copy(verticesGrowSize = x))
    opt[Int]("zigzag.garbageCollectEveryNCommands")
      .zigzagOptionAction((x, o) => o.copy(garbageCollectEveryNCommands = x))
  }

  // Parse flags.
  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())

  // Construct leader.
  val leader = new Leader[NettyTcpTransport](
    address = config.leaderAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.leaderOptions
  )

  // Construct depServiceNode.
  val depServiceNode = new DepServiceNode[NettyTcpTransport](
    address = config.depServiceNodeAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    stateMachine = flags.stateMachine,
    options = flags.depServiceNodeOptions
  )

  // Construct proposer.
  val proposer = new Proposer[NettyTcpTransport](
    address = config.proposerAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.proposerOptions
  )

  // Construct acceptor.
  val acceptor = new Acceptor[NettyTcpTransport](
    address = config.acceptorAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    options = flags.acceptorOptions
  )

  // Construct replica.
  val replica = new Replica[NettyTcpTransport](
    address = config.replicaAddresses(flags.index),
    transport = transport,
    logger = logger,
    config = config,
    stateMachine = flags.stateMachine,
    dependencyGraph = new ZigzagTarjanDependencyGraph(
      VertexIdPrefixSet(config.leaderAddresses.size),
      numLeaders = config.leaderAddresses.size,
      like = VertexIdHelpers.like,
      options = flags.zigzagOptions
    ),
    options = flags.replicaOptions
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
