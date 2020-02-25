package frankenpaxos.matchmakermultipaxos

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
      port: Int = 9000,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Benchmark flags.
      warmupDuration: java.time.Duration = java.time.Duration.ofSeconds(5),
      warmupTimeout: Duration = 10 seconds,
      warmupSleep: java.time.Duration = java.time.Duration.ofSeconds(0),
      numWarmupClients: Int = 10,
      duration: java.time.Duration = java.time.Duration.ofSeconds(5),
      timeout: Duration = 10 seconds,
      numClients: Int = 1,
      workload: Workload = new StringWorkload(0, 0),
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
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"Prometheus port; -1 to disable")

    // Benchmark flags.
    opt[java.time.Duration]("warmup_duration")
      .action((x, f) => f.copy(warmupDuration = x))
    opt[Duration]("warmup_timeout")
      .action((x, f) => f.copy(warmupTimeout = x))
    opt[java.time.Duration]("warmup_sleep")
      .action((x, f) => f.copy(warmupSleep = x))
    opt[Int]("num_warmup_clients")
      .action((x, f) => f.copy(numWarmupClients = x))
    opt[java.time.Duration]("duration")
      .action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout")
      .action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients")
      .action((x, f) => f.copy(numClients = x))
    opt[Workload]("workload")
      .action((x, f) => f.copy(workload = x))
    opt[String]("output_file_prefix")
      .action((x, f) => f.copy(outputFilePrefix = x))

    // Options.
    opt[java.time.Duration]("options.resendClientRequestPeriod")
      .optionAction((x, o) => o.copy(resendClientRequestPeriod = x))
    opt[Int]("options.stutter")
      .optionAction((x, o) => o.copy(stutter = x))
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
    config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath()),
    options = flags.options,
    metrics = new ClientMetrics(PrometheusCollectors)
  )

  // Functions to warmup and run the clients. When warming up, we don't record
  // any stats.
  def warmupRun(pseudonym: Int): Future[Unit] = {
    implicit val context = transport.executionContext
    client
      .propose(pseudonym, flags.workload.get())
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug("Request failed.")
          Future.successful(())

        case scala.util.Success(_) =>
          Future.successful(())
      })
  }

  val recorder =
    new BenchmarkUtil.Recorder(s"${flags.outputFilePrefix}_data.csv")
  def run(pseudonym: Int): Future[Unit] = {
    implicit val context = transport.executionContext
    BenchmarkUtil
      .timed(() => client.propose(pseudonym, flags.workload.get()))
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug("Request failed.")
          Future.successful(())

        case scala.util.Success((_, timing)) =>
          recorder.record(
            start = timing.startTime,
            stop = timing.stopTime,
            latencyNanos = timing.durationNanos,
            host = flags.host,
            port = flags.port
          )
          Future.successful(())
      })
  }

  // Warm up the protocol.
  implicit val context = transport.executionContext
  val warmupFutures = for (pseudonym <- 0 until flags.numWarmupClients)
    yield BenchmarkUtil.runFor(() => warmupRun(pseudonym), flags.warmupDuration)
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
  val futures = for (pseudonym <- flags.numWarmupClients until
                       flags.numWarmupClients + flags.numClients)
    yield BenchmarkUtil.runFor(() => run(pseudonym), flags.duration)
  try {
    logger.info("Clients started.")
    concurrent.Await.result(Future.sequence(futures), flags.timeout)
    logger.info("Clients finished successfully.")
  } catch {
    case e: java.util.concurrent.TimeoutException =>
      logger.warn("Client futures timed out!")
      logger.warn(e.toString())
  }

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
