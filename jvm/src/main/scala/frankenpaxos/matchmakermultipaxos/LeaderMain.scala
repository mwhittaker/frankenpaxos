package frankenpaxos.matchmakermultipaxos

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

  implicit class ElectionOptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def electionOptionAction(
        f: (A, ElectionOptions) => ElectionOptions
    ): scopt.OptionDef[A, Flags] =
      o.action(
        (x, flags) =>
          flags.copy(
            options = flags.options
              .copy(electionOptions = f(x, flags.options.electionOptions))
          )
      )
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
    opt[java.time.Duration]("options.resendMatchRequestsPeriod")
      .optionAction((x, o) => o.copy(resendMatchRequestsPeriod = x))
    opt[java.time.Duration]("options.resendReconfigurePeriod")
      .optionAction((x, o) => o.copy(resendReconfigurePeriod = x))
    opt[java.time.Duration]("options.resendPhase1asPeriod")
      .optionAction((x, o) => o.copy(resendPhase1asPeriod = x))
    opt[java.time.Duration]("options.resendPhase2asPeriod")
      .optionAction((x, o) => o.copy(resendPhase2asPeriod = x))
    opt[java.time.Duration]("options.resendExecutedWatermarkRequestsPeriod")
      .optionAction((x, o) => o.copy(resendExecutedWatermarkRequestsPeriod = x))
    opt[java.time.Duration]("options.resendPersistedPeriod")
      .optionAction((x, o) => o.copy(resendPersistedPeriod = x))
    opt[java.time.Duration]("options.resendGarbageCollectsPeriod")
      .optionAction((x, o) => o.copy(resendGarbageCollectsPeriod = x))
    opt[Int]("options.sendChosenWatermarkEveryN")
      .optionAction((x, o) => o.copy(sendChosenWatermarkEveryN = x))
    opt[Int]("options.stutter")
      .optionAction((x, o) => o.copy(stutter = x))
    opt[Boolean]("options.stallDuringMatchmaking")
      .optionAction((x, o) => o.copy(stallDuringMatchmaking = x))
    opt[Boolean]("options.stallDuringPhase1")
      .optionAction((x, o) => o.copy(stallDuringPhase1 = x))
    opt[Boolean]("options.disableGc")
      .optionAction((x, o) => o.copy(disableGc = x))
    opt[java.time.Duration]("options.election.pingPeriod")
      .electionOptionAction((x, o) => o.copy(pingPeriod = x))
    opt[java.time.Duration]("options.election.noPingTimeoutMin")
      .electionOptionAction((x, o) => o.copy(noPingTimeoutMin = x))
    opt[java.time.Duration]("options.election.noPingTimeoutMax")
      .electionOptionAction((x, o) => o.copy(noPingTimeoutMax = x))
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
    options = flags.options
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
