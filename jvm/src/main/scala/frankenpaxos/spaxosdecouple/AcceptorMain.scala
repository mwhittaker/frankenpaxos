package frankenpaxos.spaxosdecouple

import java.io.File

import frankenpaxos.{LogLevel, NettyTcpTransport, PrintLogger, PrometheusUtil}

object AcceptorMain extends App {
  case class Flags(
      // Basic flags.
      groupIndex: Int = -1,
      index: Int = -1,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: AcceptorOptions = AcceptorOptions.default
  )

  val parser = new scopt.OptionParser[Flags]("") {
    help("help")

    // Basic flags.
    opt[Int]("group_index").required().action((x, f) => f.copy(groupIndex = x))
    opt[Int]("index").required().action((x, f) => f.copy(index = x))
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"-1 to disable")
  }

  // Parse flags.
  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Construct acceptor.
  val logger = new PrintLogger(flags.logLevel)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val acceptor = new Acceptor[NettyTcpTransport](
    address = config.acceptorAddresses(flags.groupIndex)(flags.index),
    transport = new NettyTcpTransport(logger),
    logger = logger,
    config = config,
    options = flags.options
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
