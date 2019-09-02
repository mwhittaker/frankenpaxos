package frankenpaxos.simplegcbpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.TarjanDependencyGraph
import frankenpaxos.statemachine
import frankenpaxos.statemachine.KeyValueStore
import frankenpaxos.statemachine.StateMachine
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration

object ReplicaMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      stateMachine: StateMachine = new statemachine.Noop(),
      dependencyGraphFactory: VertexIdPrefixSet => DependencyGraph[
        VertexId,
        Unit,
        VertexIdPrefixSet
      ] = _ => ???,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: ReplicaOptions = ReplicaOptions.default
  )

  implicit val dependencyGraphRead =
    DependencyGraph.read[VertexId, Unit, VertexIdPrefixSet]

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, ReplicaOptions) => ReplicaOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => flags.copy(options = f(x, flags.options)))
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
    opt[
      VertexIdPrefixSet => DependencyGraph[VertexId, Unit, VertexIdPrefixSet]
    ]("dependency_graph")
      .required()
      .action((x, f) => f.copy(dependencyGraphFactory = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"-1 to disable")

    // Options.
    opt[Int]("options.commandsGrowSize")
      .optionAction((x, o) => o.copy(commandsGrowSize = x))
    opt[java.time.Duration]("options.recoverVertexTimerMinPeriod")
      .optionAction((x, o) => o.copy(recoverVertexTimerMinPeriod = x))
    opt[java.time.Duration]("options.recoverVertexTimerMaxPeriod")
      .optionAction((x, o) => o.copy(recoverVertexTimerMaxPeriod = x))
    opt[Boolean]("options.unsafeDontRecover")
      .optionAction((x, o) => o.copy(unsafeDontRecover = x))
    opt[Int]("options.executeGraphBatchSize")
      .optionAction((x, o) => o.copy(executeGraphBatchSize = x))
    opt[java.time.Duration]("options.executeGraphTimerPeriod")
      .optionAction((x, o) => o.copy(executeGraphTimerPeriod = x))
    opt[Boolean]("options.unsafeSkipGraphExecution")
      .optionAction((x, o) => o.copy(unsafeSkipGraphExecution = x))
    opt[Int]("options.numBlockers")
      .optionAction((x, o) => o.copy(numBlockers = x))
    opt[Int]("options.sendWatermarkEveryNCommands")
      .optionAction((x, o) => o.copy(sendWatermarkEveryNCommands = x))
    opt[Int]("options.sendSnapshotEveryNCommands")
      .optionAction((x, o) => o.copy(sendSnapshotEveryNCommands = x))
    opt[Boolean]("options.measureLatencies")
      .optionAction((x, o) => o.copy(measureLatencies = x))
  }

  // Parse flags.
  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Construct replica.
  val logger = new PrintLogger(flags.logLevel)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val replica = new Replica[NettyTcpTransport](
    address = config.replicaAddresses(flags.index),
    transport = new NettyTcpTransport(logger),
    logger = logger,
    config = config,
    stateMachine = flags.stateMachine,
    dependencyGraph = flags.dependencyGraphFactory(
      VertexIdPrefixSet(config.leaderAddresses.size)
    ),
    options = flags.options
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
