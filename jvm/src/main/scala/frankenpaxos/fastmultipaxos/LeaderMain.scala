package frankenpaxos.fastmultipaxos

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.statemachine
import frankenpaxos.statemachine.Flags.stateMachineTypeRead
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File

object LeaderMain extends App {
  case class Flags(
      index: Int = -1,
      paxosConfigFile: File = new File("."),
      stateMachineType: statemachine.StateMachineType = statemachine.TRegister,
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[Int]('i', "index")
      .required()
      .valueName("<index>")
      .action((x, f) => f.copy(index = x))
      .text("Leader index")

    opt[File]('c', "config")
      .required()
      .valueName("<file>")
      .action((x, f) => f.copy(paxosConfigFile = x))
      .text("Configuration file.")

    opt[statemachine.StateMachineType]('s', "state_machine")
      .valueName(statemachine.Flags.valueName)
      .action((x, f) => f.copy(stateMachineType = x))
      .text(s"State machine type (default: ${Flags().stateMachineType})")

    opt[String]("prometheus_host")
      .valueName("<host>")
      .action((x, f) => f.copy(prometheusHost = x))
      .text(s"Prometheus hostname (default: ${Flags().prometheusHost})")

    opt[Int]("prometheus_port")
      .valueName("<port>")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(
        s"Prometheus port; -1 to disable (default: ${Flags().prometheusPort})"
      )
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger);
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val address = config.leaderAddresses(flags.index)
  val stateMachine = statemachine.Flags.make(flags.stateMachineType)
  val server = new Leader[NettyTcpTransport](address,
                                             transport,
                                             logger,
                                             config,
                                             stateMachine)

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
