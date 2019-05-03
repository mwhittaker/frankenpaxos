package frankenpaxos.fastmultipaxos

import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.FileLogger
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.monitoring.PrometheusCollectors
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

object BenchmarkClientMain extends App {
  case class Flags(
      // Basic flags.
      host: String = "localhost",
      port: Int = 9000,
      paxosConfigFile: File = new File("."),
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: ClientOptions = ClientOptions.default,
      // Benchmark flags.
      duration: Duration = 5 seconds,
      timeout: Duration = 10 seconds,
      numClients: Int = 1,
      outputFilePrefix: String = ""
  )

  val parser = new scopt.OptionParser[Flags]("") {
    // Basic flags.
    opt[String]('h', "host").required().action((x, f) => f.copy(host = x))
    opt[Int]('p', "port").required().action((x, f) => f.copy(port = x))
    opt[File]('c', "config")
      .required()
      .action((x, f) => f.copy(paxosConfigFile = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"Prometheus port; -1 to disable")

    // Options.
    opt[Duration]("options.reproposePeriod")
      .action((x, f) => {
        f.copy(
          options = f.options.copy(
            reproposePeriod = java.time.Duration.ofNanos(x.toNanos)
          )
        )
      })

    // Benchmark flags.
    opt[Duration]("duration").action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout").action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients").action((x, f) => f.copy(numClients = x))
    opt[String]("output_file_prefix")
      .action((x, f) => f.copy(outputFilePrefix = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Start prometheus.
  val logger = new PrintLogger()
  val prometheusServer = if (flags.prometheusPort != -1) {
    DefaultExports.initialize()
    logger.info(
      s"Prometheus server running on ${flags.prometheusHost}:" +
        s"${flags.prometheusPort}"
    )
    Some(new HTTPServer(flags.prometheusHost, flags.prometheusPort))
  } else {
    logger.info(
      s"Prometheus server not running because a port of -1 was given."
    )
    None
  }

  // Construct client.
  val transport = new NettyTcpTransport(logger);
  val config = ConfigUtil.fromFile(flags.paxosConfigFile.getAbsolutePath())
  val metrics = new ClientMetrics(PrometheusCollectors)
  val client = new Client[NettyTcpTransport](
    NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    transport,
    new FileLogger(s"${flags.outputFilePrefix}.txt"),
    config,
    flags.options,
    metrics
  )

  // Run clients.
  val latencyWriter =
    CSVWriter.open(new File(s"${flags.outputFilePrefix}_data.csv"))
  latencyWriter.writeRow(Seq("start", "stop", "latency_nanos", "host", "port"))
  val stopTime = java.time.Instant
    .now()
    .plus(java.time.Duration.ofNanos(flags.duration.toNanos))

  def f(
      pseudonym: Int,
      startTime: java.time.Instant,
      startTimeNanos: Long
  )(
      reply: Try[Array[Byte]]
  ): Future[Unit] = {
    reply match {
      case scala.util.Failure(_) =>
        logger.debug("Fast Multipaxos request failed.")

      case scala.util.Success(_) =>
        val stopTimeNanos = System.nanoTime()
        val stopTime = java.time.Instant.now()
        latencyWriter.writeRow(
          Seq(
            startTime.toString(),
            stopTime.toString(),
            (stopTimeNanos - startTimeNanos).toString(),
            flags.host,
            flags.port
          )
        )
    }

    if (java.time.Instant.now().isBefore(stopTime)) {
      client
        .propose(pseudonym = pseudonym, ".".getBytes())
        .transformWith(
          f(pseudonym, java.time.Instant.now(), System.nanoTime())
        )(transport.executionContext)
    } else {
      Future.successful(())
    }
  }

  val futures = for (pseudonym <- 0 to flags.numClients) yield {
    client
      .propose(pseudonym = pseudonym, ".".getBytes())
      .transformWith(f(pseudonym, java.time.Instant.now(), System.nanoTime()))(
        transport.executionContext
      )
  }

  // Wait for the benchmark to finish.
  concurrent.Await.result(
    Future.sequence(futures)(implicitly, transport.executionContext),
    flags.timeout
  )

  logger.debug("Shutting down transport.")
  transport.shutdown()
  logger.debug("Transport shut down.")

  logger.info("Stopping prometheus.")
  prometheusServer.foreach(_.stop())
  logger.info("Prometheus stopped.")
}
