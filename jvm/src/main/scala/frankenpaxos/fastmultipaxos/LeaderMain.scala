package frankenpaxos.fastmultipaxos

import ThriftySystem.Flags.thriftySystemTypeRead
import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.statemachine
import frankenpaxos.statemachine.Flags.stateMachineTypeRead
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import scala.concurrent.duration

object LeaderMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      configFilename: File = new File("."),
      stateMachineType: statemachine.StateMachineType = statemachine.TRegister,
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: LeaderOptions = LeaderOptions.default
  )

  val parser = new scopt.OptionParser[Flags]("") {
    help("help")

    // Basic flags.
    opt[Int]("index")
      .required()
      .action((x, f) => f.copy(index = x))

    opt[File]('c', "config")
      .required()
      .action((x, f) => f.copy(configFilename = x))

    opt[statemachine.StateMachineType]('s', "state_machine")
      .action((x, f) => f.copy(stateMachineType = x))

    opt[String]("log_level")
      .valueName("<debug | info | warn | error | fatal>")
      .validate(x => {
        x match {
          case "debug" | "info" | "warn" | "error" | "fatal" => success
          case _ =>
            failure(s"$x is not debug, info, warn, error, or fatal.")
        }
      })
      .action((x, f) => {
        x match {
          case "debug" => f.copy(logLevel = frankenpaxos.LogDebug)
          case "info"  => f.copy(logLevel = frankenpaxos.LogInfo)
          case "warn"  => f.copy(logLevel = frankenpaxos.LogWarn)
          case "error" => f.copy(logLevel = frankenpaxos.LogError)
          case "fatal" => f.copy(logLevel = frankenpaxos.LogFatal)
          case _       => ???
        }
      })

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))

    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text("-1 to disable")

    // Options.
    opt[ThriftySystem.Flags.ThriftySystemType]("options.thriftySystem")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            thriftySystem = ThriftySystem.Flags.make(x)
          )
        )
      })
      .valueName(ThriftySystem.Flags.valueName)

    opt[duration.Duration]("options.resendPhase1asTimerPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            resendPhase1asTimerPeriod = java.time.Duration.ofNanos(x.toNanos)
          )
        )
      })

    opt[duration.Duration]("options.resendPhase2asTimerPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            resendPhase2asTimerPeriod = java.time.Duration.ofNanos(x.toNanos)
          )
        )
      })

    opt[Int]("options.phase2aMaxBufferSize")
      .action((x, f) => {
        f.copy(options = f.options.copy(phase2aMaxBufferSize = x))
      })

    opt[duration.Duration]("options.phase2aBufferFlushPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options
            .copy(
              phase2aBufferFlushPeriod = java.time.Duration.ofNanos(x.toNanos)
            )
        )
      })

    opt[Int]("options.valueChosenMaxBufferSize")
      .action((x, f) => {
        f.copy(options = f.options.copy(valueChosenMaxBufferSize = x))
      })

    opt[duration.Duration]("options.valueChosenBufferFlushPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            valueChosenBufferFlushPeriod = java.time.Duration.ofNanos(x.toNanos)
          )
        )
      })

    opt[duration.Duration]("options.election.pingPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            leaderElectionOptions = f.options.leaderElectionOptions.copy(
              pingPeriod = java.time.Duration.ofNanos(x.toNanos)
            )
          )
        )
      })

    opt[duration.Duration]("options.election.noPingTimeoutMin")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            leaderElectionOptions = f.options.leaderElectionOptions.copy(
              noPingTimeoutMin = java.time.Duration.ofNanos(x.toNanos)
            )
          )
        )
      })

    opt[duration.Duration]("options.election.noPingTimeoutMax")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            leaderElectionOptions = f.options.leaderElectionOptions.copy(
              noPingTimeoutMax = java.time.Duration.ofNanos(x.toNanos)
            )
          )
        )
      })

    opt[duration.Duration]("options.election.notEnoughVotesTimeoutMin")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            leaderElectionOptions = f.options.leaderElectionOptions.copy(
              notEnoughVotesTimeoutMin = java.time.Duration.ofNanos(x.toNanos)
            )
          )
        )
      })

    opt[duration.Duration]("options.election.notEnoughVotesTimeoutMax")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            leaderElectionOptions = f.options.leaderElectionOptions.copy(
              notEnoughVotesTimeoutMax = java.time.Duration.ofNanos(x.toNanos)
            )
          )
        )
      })

    opt[duration.Duration]("options.heartbeat.failPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            heartbeatOptions = f.options.heartbeatOptions.copy(
              failPeriod = java.time.Duration.ofNanos(x.toNanos)
            )
          )
        )
      })

    opt[duration.Duration]("options.heartbeat.successPeriod")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            heartbeatOptions = f.options.heartbeatOptions.copy(
              successPeriod = java.time.Duration.ofNanos(x.toNanos)
            )
          )
        )
      })

    opt[Int]("options.heartbeat.numRetries")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            heartbeatOptions = f.options.heartbeatOptions.copy(
              numRetries = x
            )
          )
        )
      })

    opt[Double]("options.heartbeat.networkDelayAlpha")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            heartbeatOptions = f.options.heartbeatOptions.copy(
              networkDelayAlpha = x
            )
          )
        )
      })
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger)
  val config = ConfigUtil.fromFile(flags.configFilename.getAbsolutePath())
  val address = config.leaderAddresses(flags.index)
  val stateMachine = statemachine.Flags.make(flags.stateMachineType)
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
