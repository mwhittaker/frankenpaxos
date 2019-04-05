package frankenpaxos.echo

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.net.InetAddress
import java.net.InetSocketAddress

object BenchmarkServerMain extends App {
  case class Flags(
      host: String = "localhost",
      port: Int = 9000,
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]("host")
      .valueName("<host>")
      .action((x, f) => f.copy(host = x))
      .text(s"Hostname (default: ${Flags().host})")

    opt[Int]("port")
      .valueName("<port>")
      .action((x, f) => f.copy(port = x))
      .text(s"Port (default: ${Flags().port})")

    opt[String]("prometheus_host")
      .valueName("<host>")
      .action((x, f) => f.copy(prometheusHost = x))
      .text(s"Prometheus hostname (default: ${Flags().prometheusHost})")

    opt[Int]("prometheus_port")
      .valueName("<port>")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"Prometheus port (default: ${Flags().prometheusPort})")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) => flags
    case None        => ???
  }

  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger)
  val address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port))
  val server =
    new BenchmarkServer[NettyTcpTransport](address, transport, logger)

  DefaultExports.initialize()
  val prometheusServer =
    new HTTPServer(flags.prometheusHost, flags.prometheusPort)
  logger.info(
    s"Prometheus server running on ${flags.prometheusHost}:${flags.prometheusPort}"
  )
}
