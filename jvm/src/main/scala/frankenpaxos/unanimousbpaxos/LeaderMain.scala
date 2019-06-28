package frankenpaxos.unanimousbpaxos

import VertexIdHelpers.vertexIdOrdering
import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.depgraph.JgraphtDependencyGraph
import frankenpaxos.statemachine.KeyValueStore
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration

object LeaderMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: LeaderOptions = LeaderOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, LeaderOptions) => LeaderOptions
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
    opt[java.time.Duration]("options.resendDependencyRequestsTimerPeriod")
      .optionAction((x, o) => o.copy(resendDependencyRequestsTimerPeriod = x))
    opt[java.time.Duration]("options.resendPhase1asTimerPeriod")
      .optionAction((x, o) => o.copy(resendPhase1asTimerPeriod = x))
    opt[java.time.Duration]("options.resendPhase2asTimerPeriod")
      .optionAction((x, o) => o.copy(resendPhase2asTimerPeriod = x))
    opt[java.time.Duration]("options.recoverVertexTimerMinPeriod")
      .optionAction((x, o) => o.copy(recoverVertexTimerMinPeriod = x))
    opt[java.time.Duration]("options.recoverVertexTimerMaxPeriod")
      .optionAction((x, o) => o.copy(recoverVertexTimerMaxPeriod = x))
  }

  // Parse flags.
  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Construct leader.
  val logger = new PrintLogger(flags.logLevel)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val leader = new Leader[NettyTcpTransport](
    address = config.leaderAddresses(flags.index),
    transport = new NettyTcpTransport(logger),
    logger = logger,
    config = config,
    stateMachine = new KeyValueStore(),
    dependencyGraph = new JgraphtDependencyGraph(),
    options = flags.options
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
