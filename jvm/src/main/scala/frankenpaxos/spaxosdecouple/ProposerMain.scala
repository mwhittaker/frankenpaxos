package frankenpaxos.spaxosdecouple

import java.io.File

import frankenpaxos.{NettyTcpTransport, PrintLogger}
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports

import scala.concurrent.duration

object ProposerMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      paxosConfigFile: File = new File("."),
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: ProposerOptions = ProposerOptions.default
  )

  val parser = new scopt.OptionParser[Flags]("") {
    // Basic flags.
    opt[Int]('i', "index")
      .required()
      .action((x, f) => f.copy(index = x))

    opt[File]('c', "config")
      .required()
      .action((x, f) => f.copy(paxosConfigFile = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))

    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"-1 to disable")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger)
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val address = config.acceptorAddresses(flags.index)
  val proposer = new Proposer[NettyTcpTransport](address,
                                                 transport,
                                                 logger,
                                                 config,
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
