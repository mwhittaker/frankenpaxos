package frankenpaxos.fastmultipaxos

import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.election.LeaderElectionOptions
import frankenpaxos.heartbeat.HeartbeatOptions
import frankenpaxos.statemachine
import frankenpaxos.statemachine.Sleeper
import frankenpaxos.thrifty.ThriftySystem
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
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

  implicit class ElectionWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def electionAction(
        f: (A, LeaderElectionOptions) => LeaderElectionOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => {
        flags.copy(
          options = flags.options.copy(
            leaderElectionOptions = f(x, flags.options.leaderElectionOptions)
          )
        )
      })
  }

  implicit class HeartbeatWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def heartbeatAction(
        f: (A, HeartbeatOptions) => HeartbeatOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => {
        flags.copy(
          options = flags.options.copy(
            heartbeatOptions = f(x, flags.options.heartbeatOptions)
          )
        )
      })
  }

  val parser = new scopt.OptionParser[Flags]("") {
    help("help")

    // Basic flags.
    opt[Int]("index").required().action((x, f) => f.copy(index = x))
    opt[File]('c', "config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text("-1 to disable")

    // Options.
    opt[ThriftySystem]("options.thriftySystem")
      .optionAction((x, o) => o.copy(thriftySystem = x))
    opt[java.time.Duration]("options.resendPhase1asTimerPeriod")
      .optionAction((x, o) => o.copy(resendPhase1asTimerPeriod = x))
    opt[java.time.Duration]("options.resendPhase2asTimerPeriod")
      .optionAction((x, o) => o.copy(resendPhase2asTimerPeriod = x))
    opt[Int]("options.phase2aMaxBufferSize")
      .optionAction((x, o) => o.copy(phase2aMaxBufferSize = x))
    opt[java.time.Duration]("options.phase2aBufferFlushPeriod")
      .optionAction((x, o) => o.copy(phase2aBufferFlushPeriod = x))
    opt[Int]("options.valueChosenMaxBufferSize")
      .optionAction((x, o) => o.copy(valueChosenMaxBufferSize = x))
    opt[java.time.Duration]("options.valueChosenBufferFlushPeriod")
      .optionAction((x, o) => o.copy(valueChosenBufferFlushPeriod = x))

    opt[java.time.Duration]("options.election.pingPeriod")
      .electionAction((x, e) => e.copy(pingPeriod = x))
    opt[java.time.Duration]("options.election.noPingTimeoutMin")
      .electionAction((x, e) => e.copy(noPingTimeoutMin = x))
    opt[java.time.Duration]("options.election.noPingTimeoutMax")
      .electionAction((x, e) => e.copy(noPingTimeoutMax = x))
    opt[java.time.Duration]("options.election.notEnoughVotesTimeoutMin")
      .electionAction((x, e) => e.copy(notEnoughVotesTimeoutMin = x))
    opt[java.time.Duration]("options.election.notEnoughVotesTimeoutMax")
      .electionAction((x, e) => e.copy(notEnoughVotesTimeoutMax = x))

    opt[java.time.Duration]("options.heartbeat.failPeriod")
      .heartbeatAction((x, h) => h.copy(failPeriod = x))
    opt[java.time.Duration]("options.heartbeat.successPeriod")
      .heartbeatAction((x, h) => h.copy(successPeriod = x))
    opt[Int]("options.heartbeat.numRetries")
      .heartbeatAction((x, h) => h.copy(numRetries = x))
    opt[Double]("options.heartbeat.networkDelayAlpha")
      .heartbeatAction((x, h) => h.copy(networkDelayAlpha = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val address = config.leaderAddresses(flags.index)
  // This benchmark hard codes the use of the sleeper state machine.
  val stateMachine = new Sleeper()
  val server = new Leader[NettyTcpTransport](address,
                                             transport,
                                             logger,
                                             config,
                                             stateMachine,
                                             flags.options)

  if (flags.prometheusPort != -1) {
    DefaultExports.initialize()
    val prometheusServer =
      new HTTPServer(flags.prometheusHost, flags.prometheusPort)
    logger.info(
      s"Prometheus server running on ${flags.prometheusHost}:" +
        s"${flags.prometheusPort}"
    )
  } else {
    logger.info(
      s"Prometheus server not running because a port of -1 was given."
    )
  }
}
