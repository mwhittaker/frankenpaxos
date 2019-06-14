package frankenpaxos.fastmultipaxos

import com.github.tototoshi.csv.CSVWriter
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.FileLogger
import frankenpaxos.Flags.durationRead
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.statemachine.SleepRequest
import frankenpaxos.statemachine.SleeperInput
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import scala.util.Try

object BenchmarkClientMain extends App {
  case class Flags(
      // Basic flags.
      host: String = "localhost",
      port: Int = 9000,
      paxosConfig: File = new File("."),
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Benchmark flags.
      duration: Duration = 5 seconds,
      timeout: Duration = 10 seconds,
      numClients: Int = 1,
      commandSizeBytesMean: Int = 100,
      commandSizeBytesStddev: Int = 0,
      sleepTimeNanosMean: Int = 1,
      sleepTimeNanosStddev: Int = 0,
      outputFilePrefix: String = "",
      // Options.
      options: ClientOptions = ClientOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, ClientOptions) => ClientOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => flags.copy(options = f(x, flags.options)))
  }

  val parser = new scopt.OptionParser[Flags]("") {
    // Basic flags.
    opt[String]("host").required().action((x, f) => f.copy(host = x))
    opt[Int]("port").required().action((x, f) => f.copy(port = x))
    opt[File]("config").required().action((x, f) => f.copy(paxosConfig = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"Prometheus port; -1 to disable")

    // Benchmark flags.
    opt[Duration]("duration")
      .action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout")
      .action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients")
      .action((x, f) => f.copy(numClients = x))
    opt[Int]("command_size_bytes_mean")
      .action((x, f) => f.copy(commandSizeBytesMean = x))
    opt[Int]("command_size_bytes_stddev")
      .action((x, f) => f.copy(commandSizeBytesStddev = x))
    opt[Int]("sleep_time_nanos_mean")
      .action((x, f) => f.copy(sleepTimeNanosMean = x))
    opt[Int]("sleep_time_nanos_stddev")
      .action((x, f) => f.copy(sleepTimeNanosStddev = x))
    opt[String]("output_file_prefix")
      .action((x, f) => f.copy(outputFilePrefix = x))

    // Options.
    opt[java.time.Duration]("options.reproposePeriod")
      .optionAction((x, o) => o.copy(reproposePeriod = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Helper function to generate command.
  def randomProposal(): SleeperInput = {
    var commandSizeBytes: Int =
      (Random.nextGaussian() * flags.commandSizeBytesStddev).toInt
    commandSizeBytes += flags.commandSizeBytesMean
    commandSizeBytes = Math.max(0, commandSizeBytes)

    var sleepTimeNanos: Int =
      (Random.nextGaussian() * flags.sleepTimeNanosStddev).toInt
    sleepTimeNanos += flags.sleepTimeNanosMean
    sleepTimeNanos = Math.max(0, sleepTimeNanos)

    SleeperInput().withSleepRequest(
      SleepRequest(sleepNanos = sleepTimeNanos,
                   padding =
                     ByteString.copyFrom(Array.fill[Byte](commandSizeBytes)(0)))
    )
  }

  // Start prometheus.
  val prometheusServer =
    PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)

  // Construct client.
  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger);
  val config = ConfigUtil.fromFile(flags.paxosConfig.getAbsolutePath())
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
        .propose(pseudonym = pseudonym, randomProposal().toByteArray)
        .transformWith(
          f(pseudonym, java.time.Instant.now(), System.nanoTime())
        )(transport.executionContext)
    } else {
      Future.successful(())
    }
  }

  val futures = for (pseudonym <- 0 to flags.numClients) yield {
    client
      .propose(pseudonym = pseudonym, randomProposal().toByteArray)
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
