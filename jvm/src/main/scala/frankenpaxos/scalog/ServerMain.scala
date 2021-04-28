package frankenpaxos.scalog

import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.statemachine.AppendLog
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration

object ServerMain extends App {
  case class Flags(
      // Basic flags.
      shardIndex: Int = -1,
      index: Int = -1,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: ServerOptions = ServerOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, ServerOptions) => ServerOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => flags.copy(options = f(x, flags.options)))
  }

  val parser = new scopt.OptionParser[Flags]("") {
    help("help")

    // Basic flags.
    opt[Int]("shard_index").required().action((x, f) => f.copy(shardIndex = x))
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
    opt[java.time.Duration]("options.pushPeriod")
      .optionAction((x, o) => o.copy(pushPeriod = x))
    opt[java.time.Duration]("options.recoverPeriod")
      .optionAction((x, o) => o.copy(recoverPeriod = x))
    opt[Int]("options.logGrowSize")
      .optionAction((x, o) => o.copy(logGrowSize = x))
    opt[Boolean]("options.unsafeDontRecover")
      .optionAction((x, o) => o.copy(unsafeDontRecover = x))
  }

  // Parse flags.
  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Construct server.
  val logger = new PrintLogger(flags.logLevel)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val server = new Server[NettyTcpTransport](
    address = config.serverAddresses(flags.shardIndex)(flags.index),
    transport = new NettyTcpTransport(logger),
    logger = logger,
    config = config,
    options = flags.options
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
