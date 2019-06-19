package frankenpaxos.epaxos

import InstanceHelpers.instanceOrdering
import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.statemachine.KeyValueStore
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
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: ReplicaOptions = ReplicaOptions.default
  )

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

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"-1 to disable")

    // Options.
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
    stateMachine = new KeyValueStore(),
    dependencyGraph = new JgraphtDependencyGraph(),
    options = flags.options
  )

  // Start Prometheus.
  if (flags.prometheusPort != -1) {
    DefaultExports.initialize()
    val prometheusServer =
      new HTTPServer(flags.prometheusHost, flags.prometheusPort)
  }
}
