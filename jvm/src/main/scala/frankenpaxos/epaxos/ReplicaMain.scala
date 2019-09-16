package frankenpaxos.epaxos

import InstanceHelpers.instanceOrdering
import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.depgraph.DependencyGraph
import frankenpaxos.depgraph.TarjanDependencyGraph
import frankenpaxos.depgraph.ZigzagTarjanDependencyGraph
import frankenpaxos.depgraph.ZigzagTarjanDependencyGraphOptions
import frankenpaxos.statemachine
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.thrifty.ThriftySystem
import frankenpaxos.util
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
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: ReplicaOptions = ReplicaOptions.default,
      zigzagOptions: ZigzagTarjanDependencyGraphOptions =
        ZigzagTarjanDependencyGraphOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, ReplicaOptions) => ReplicaOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => flags.copy(options = f(x, flags.options)))
  }

  implicit class ZigzagOptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
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

    // Options.
    opt[ThriftySystem]("options.thriftySystem")
      .optionAction((x, o) => o.copy(thriftySystem = x))
    opt[java.time.Duration]("options.resendPreAcceptsTimerPeriod")
      .optionAction((x, o) => o.copy(resendPreAcceptsTimerPeriod = x))
    opt[java.time.Duration]("options.defaultToSlowPathTimerPeriod")
      .optionAction((x, o) => o.copy(defaultToSlowPathTimerPeriod = x))
    opt[java.time.Duration]("options.resendAcceptsTimerPeriod")
      .optionAction((x, o) => o.copy(resendAcceptsTimerPeriod = x))
    opt[java.time.Duration]("options.resendPreparesTimerPeriod")
      .optionAction((x, o) => o.copy(resendPreparesTimerPeriod = x))
    opt[java.time.Duration]("options.recoverInstanceTimerMinPeriod")
      .optionAction((x, o) => o.copy(recoverInstanceTimerMinPeriod = x))
    opt[java.time.Duration]("options.recoverInstanceTimerMaxPeriod")
      .optionAction((x, o) => o.copy(recoverInstanceTimerMaxPeriod = x))
    opt[Boolean]("options.unsafeSkipGraphExecution")
      .optionAction((x, o) => o.copy(unsafeSkipGraphExecution = x))
    opt[Int]("options.executeGraphBatchSize")
      .optionAction((x, o) => o.copy(executeGraphBatchSize = x))
    opt[java.time.Duration]("options.executeGraphTimerPeriod")
      .optionAction((x, o) => o.copy(executeGraphTimerPeriod = x))
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

  // Construct replica.
  val logger = new PrintLogger(flags.logLevel)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val replica = new Replica[NettyTcpTransport](
    address = config.replicaAddresses(flags.index),
    transport = new NettyTcpTransport(logger),
    logger = logger,
    config = config,
    stateMachine = flags.stateMachine,
    dependencyGraph = new ZigzagTarjanDependencyGraph(
      InstancePrefixSet(config.replicaAddresses.size),
      numLeaders = config.replicaAddresses.size,
      like = InstanceHelpers.like,
      options = flags.zigzagOptions
    ),
    options = flags.options
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
