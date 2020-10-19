package frankenpaxos.unreplicated

import frankenpaxos.Actor
import frankenpaxos.BenchmarkUtil
import frankenpaxos.FileLogger
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.StringWorkload
import frankenpaxos.Workload
import frankenpaxos.monitoring.PrometheusCollectors
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random

object ClientMain extends App {
  case class Flags(
      // Basic flags.
      host: String = "localhost",
      port: Int = 0,
      serverHost: String = "localhost",
      serverPort: Int = 0,
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 0,
      // Benchmark flags.
      measurementGroupSize: Int = 1,
      warmupDuration: java.time.Duration = java.time.Duration.ofSeconds(5),
      warmupTimeout: Duration = 10 seconds,
      warmupSleep: java.time.Duration = java.time.Duration.ofSeconds(0),
      numWarmupClients: Int = 1,
      duration: java.time.Duration = java.time.Duration.ofSeconds(5),
      timeout: Duration = 10 seconds,
      numClients: Int = 1,
      workload: Workload = new StringWorkload(0, 0),
      outputFilePrefix: String = "",
      // Options.
      options: ClientOptions = ClientOptions.default
  )

  val parser = new scopt.OptionParser[Flags]("") {
    // Basic flags.
    opt[String]("host").required().action((x, f) => f.copy(host = x))
    opt[Int]("port").required().action((x, f) => f.copy(port = x))
    opt[String]("server_host")
      .required()
      .action((x, f) => f.copy(serverHost = x))
    opt[Int]("server_port").required().action((x, f) => f.copy(serverPort = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"Prometheus port; -1 to disable")

    // Benchmark flags.
    opt[Int]("measurement_group_size")
      .action((x, f) => f.copy(measurementGroupSize = x))
    opt[java.time.Duration]("warmup_duration")
      .required()
      .action((x, f) => f.copy(warmupDuration = x))
    opt[Duration]("warmup_timeout")
      .required()
      .action((x, f) => f.copy(warmupTimeout = x))
    opt[java.time.Duration]("warmup_sleep")
      .required()
      .action((x, f) => f.copy(warmupSleep = x))
    opt[Int]("num_warmup_clients")
      .required()
      .action((x, f) => f.copy(numWarmupClients = x))
    opt[java.time.Duration]("duration")
      .required()
      .action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout")
      .required()
      .action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients")
      .required()
      .action((x, f) => f.copy(numClients = x))
    opt[Workload]("workload")
      .required()
      .action((x, f) => f.copy(workload = x))
    opt[String]("output_file_prefix")
      .required()
      .action((x, f) => f.copy(outputFilePrefix = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Start prometheus.
  val prometheusServer =
    PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)

  // Construct client.
  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger)
  val client = new Client[NettyTcpTransport](
    address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    transport = transport,
    logger = logger,
    serverAddress = NettyTcpAddress(
      new InetSocketAddress(flags.serverHost, flags.serverPort)
    ),
    options = flags.options,
    metrics = new ClientMetrics(PrometheusCollectors)
  )

  // Functions to warmup and run the clients. When warming up, we don't record
  // any stats.
  def warmupRun(): Future[Unit] = {
    implicit val context = transport.executionContext
    client
      .propose(flags.workload.get())
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug("Request failed.")
          Future.successful(())

        case scala.util.Success(_) =>
          Future.successful(())
      })
  }

  // val recorder =
  //   new BenchmarkUtil.Recorder(s"${flags.outputFilePrefix}_data.csv")
  val recorder = new BenchmarkUtil.LabeledRecorder(
    s"${flags.outputFilePrefix}_data.csv",
    groupSize = flags.measurementGroupSize
  )
  def run(): Future[Unit] = {
    implicit val context = transport.executionContext
    BenchmarkUtil
      .timed(() => client.propose(flags.workload.get()))
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug("Request failed.")
          Future.successful(())

        case scala.util.Success((_, timing)) =>
          recorder.record(
            start = timing.startTime,
            stop = timing.stopTime,
            latencyNanos = timing.durationNanos,
            label = "write"
          )
          Future.successful(())
      })
  }

  // Warm up the protocol.
  implicit val context = transport.executionContext
  val warmupFutures = for (_ <- 0 to flags.numWarmupClients)
    yield BenchmarkUtil.runFor(() => warmupRun(), flags.warmupDuration)
  try {
    logger.info("Client warmup started.")
    concurrent.Await.result(Future.sequence(warmupFutures), flags.warmupTimeout)
    logger.info("Client warmup finished successfully.")
  } catch {
    case e: java.util.concurrent.TimeoutException =>
      logger.warn("Client warmup futures timed out!")
      logger.warn(e.toString())
  }

  // Sleep to let protocol settle.
  Thread.sleep(flags.warmupSleep.toMillis())

  // Run the benchmark.
  val futures = for (_ <- 0 to flags.numClients)
    yield BenchmarkUtil.runFor(() => run(), flags.duration)
  try {
    logger.info("Clients started.")
    concurrent.Await.result(Future.sequence(futures), flags.timeout)
    logger.info("Clients finished successfully.")
  } catch {
    case e: java.util.concurrent.TimeoutException =>
      logger.warn("Client futures timed out!")
      logger.warn(e.toString())
  }
  recorder.flush()

  // Shut everything down.
  logger.info("Shutting down transport.")
  transport.shutdown()
  logger.info("Transport shut down.")

  prometheusServer.foreach(server => {
    logger.info("Stopping prometheus.")
    server.stop()
    logger.info("Prometheus stopped.")
  })
}
