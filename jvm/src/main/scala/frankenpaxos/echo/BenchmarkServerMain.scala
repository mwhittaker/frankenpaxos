package frankenpaxos.echo

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
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
    opt[String]("host").action((x, f) => f.copy(host = x))
    opt[Int]("port").action((x, f) => f.copy(port = x))
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text("-1 to disable")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Start the server.
  val logger = new PrintLogger()
  val server = new BenchmarkServer[NettyTcpTransport](
    address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    transport = new NettyTcpTransport(logger),
    logger = logger
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
